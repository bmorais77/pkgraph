package org.apache.spark.graphx.pkgraph.graph.impl

import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.{EdgePartition, EdgePartitionBuilder, EdgeRDDImpl}
import org.apache.spark.graphx.pkgraph.graph.PKEdgeRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, Partition, Partitioner, TaskContext}

import scala.reflect.ClassTag

private[graph] class PKEdgeRDDImpl[V: ClassTag, E: ClassTag](
    val edgePartitions: RDD[(PartitionID, PKEdgePartition[V, E])],
    val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
) extends PKEdgeRDD[E](edgePartitions) {

  override def compute(part: Partition, context: TaskContext): Iterator[Edge[E]] = {
    val p = firstParent[(PartitionID, PKEdgePartition[_, E])].iterator(part, context)
    if (p.hasNext) {
      val partition = p.next()._2
      partition.iterator.map(_.copy())
    } else {
      Iterator.empty
    }
  }

  override def setName(_name: String): this.type = {
    if (edgePartitions.name != null) {
      edgePartitions.setName(edgePartitions.name + ", " + _name)
    } else {
      edgePartitions.setName(_name)
    }
    this
  }

  setName("PKEdgeRDD")

  /**
    * If `partitionsRDD` already has a partitioner, use it. Otherwise assume that the
    * `PartitionID`s in `partitionsRDD` correspond to the actual partitions and create a new
    * partitioner that allows co-partitioning with `partitionsRDD`.
    */
  override val partitioner: Option[Partitioner] =
    edgePartitions.partitioner.orElse(Some(new HashPartitioner(partitions.length)))

  override def collect(): Array[Edge[E]] = map(_.copy()).collect()

  /**
    * Persists the edge partitions at the specified storage level, ignoring any existing target
    * storage level.
    */
  override def persist(newLevel: StorageLevel): this.type = {
    edgePartitions.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = false): this.type = {
    edgePartitions.unpersist(blocking)
    this
  }

  /**
    * Persists the edge partitions using `targetStorageLevel`, which defaults to MEMORY_ONLY.
    */
  override def cache(): this.type = {
    edgePartitions.persist(targetStorageLevel)
    this
  }

  override def getStorageLevel: StorageLevel = edgePartitions.getStorageLevel

  override def checkpoint(): Unit = edgePartitions.checkpoint()

  override def isCheckpointed: Boolean = {
    firstParent[(PartitionID, PKEdgePartition[V, E])].isCheckpointed
  }

  override def getCheckpointFile: Option[String] = {
    edgePartitions.getCheckpointFile
  }

  /** The number of edges in the RDD. */
  override def count(): Long = {
    edgePartitions.map(_._2.size.toLong).fold(0)(_ + _)
  }

  override def getPartitions: Array[Partition] = edgePartitions.partitions

  def mapValues[E2: ClassTag](f: Edge[E] => E2): PKEdgeRDDImpl[V, E2] =
    mapEdgePartitions[V, E2]((_, part) => part.map(f))

  def reverse: PKEdgeRDDImpl[V, E] = mapEdgePartitions[V, E]((_, part) => part.reverse)

  def filter(epred: EdgeTriplet[V, E] => Boolean, vpred: (VertexId, V) => Boolean): PKEdgeRDDImpl[V, E] = {
    mapEdgePartitions[V, E]((_, part) => part.filter(epred, vpred))
  }

  override def innerJoin[E2: ClassTag, E3: ClassTag](
      other: EdgeRDD[E2]
  )(f: (VertexId, VertexId, E, E2) => E3): PKEdgeRDDImpl[V, E3] = {
    val otherRDD = other.asInstanceOf[PKEdgeRDDImpl[V, E2]]
    val partitions = edgePartitions
      .zipPartitions(otherRDD.edgePartitions, preservesPartitioning = true) { (thisIter, otherIter) =>
        val (pid, thisEPart) = thisIter.next()
        val (_, otherEPart) = otherIter.next()
        Iterator(Tuple2(pid, thisEPart.innerJoin(otherEPart)(f)))
      }

    new PKEdgeRDDImpl[V, E3](partitions, targetStorageLevel)
  }

  /**
    * Maps each edge partition according to the given user function.
    *
    * @param f User function
    * @tparam V2 new type of vertex attributes
    * @tparam E2 new type of edge attributes
    * @return new [[PKEdgeRDD]] with mapped edge partitions
    */
  def mapEdgePartitions[V2: ClassTag, E2: ClassTag](
      f: (PartitionID, PKEdgePartition[V, E]) => PKEdgePartition[V, E2]
  ): PKEdgeRDDImpl[V, E2] = {
    val partitions = edgePartitions.mapPartitions(
      { iter =>
        if (iter.hasNext) {
          val (pid, ep) = iter.next()
          Iterator(Tuple2(pid, f(pid, ep)))
        } else {
          Iterator.empty
        }
      },
      preservesPartitioning = true
    )
    new PKEdgeRDDImpl(partitions, targetStorageLevel)
  }

  /**
    * Builds a new [[PKEdgeRDDImpl]] with the same data as this one, but using the given edge partitions.
    *
    * @param partitionsRDD New edge partitions to use
    * @tparam V2 New type of vertex attributes
    * @tparam E2 New type of edge attributes
    * @return [[PKEdgeRDDImpl]] with given edge partitions
    */
  def withEdgePartitions[V2: ClassTag, E2: ClassTag](
      partitionsRDD: RDD[(PartitionID, PKEdgePartition[V2, E2])]
  ): PKEdgeRDDImpl[V2, E2] = {
    new PKEdgeRDDImpl(partitionsRDD, targetStorageLevel)
  }

  override def withTargetStorageLevel(targetStorageLevel: StorageLevel): PKEdgeRDDImpl[V, E] = {
    new PKEdgeRDDImpl(edgePartitions, targetStorageLevel)
  }
}

object PKEdgeRDDImpl {

  /**
    * Creates an [[PKEdgeRDDImpl]] from a set of edges.
    *
    * @tparam V the type of the vertex attributes that may be joined with the returned EdgeRDD
    * @tparam E the edge attribute type
    *
    * @param edges to create RDD from
    */
  def fromEdges[V: ClassTag, E: ClassTag](edges: RDD[Edge[E]]): PKEdgeRDDImpl[V, E] = {
    val edgePartitions = edges.mapPartitionsWithIndex { (pid, iter) =>
      val builder = PKEdgePartitionBuilder[V, E](2) // TODO: user should supply k
      iter.foreach { e =>
        builder.add(e.srcId, e.dstId, e.attr)
      }
      Iterator((pid, builder.build))
    }
    fromEdgePartitions(edgePartitions)
  }

  /**
    * Creates an [[PKEdgeRDDImpl]] from already-constructed edge partitions.
    *
    * @tparam V the type of the vertex attributes that may be joined with the returned EdgeRDD
    * @tparam E the edge attribute type
    */
  def fromEdgePartitions[V: ClassTag, E: ClassTag](
      edgePartitions: RDD[(Int, PKEdgePartition[V, E])]
  ): PKEdgeRDDImpl[V, E] = {
    new PKEdgeRDDImpl(edgePartitions)
  }
}
