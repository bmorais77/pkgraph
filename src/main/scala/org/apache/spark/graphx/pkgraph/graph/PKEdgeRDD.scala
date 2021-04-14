package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

private[graph] class PKEdgeRDD[V: ClassTag, E: ClassTag](
    val edgePartitions: RDD[(PartitionID, PKEdgePartition[V, E])],
    val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
) extends EdgeRDD[E](edgePartitions.context, List(new OneToOneDependency(edgePartitions))) {

  override def partitionsRDD = throw new UnsupportedOperationException

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

  def mapValues[E2: ClassTag](f: Edge[E] => E2): PKEdgeRDD[V, E2] =
    mapEdgePartitions[V, E2]((_, part) => part.map(f))

  def reverse: PKEdgeRDD[V, E] = mapEdgePartitions[V, E]((_, part) => part.reverse)

  def filter(epred: EdgeTriplet[V, E] => Boolean, vpred: (VertexId, V) => Boolean): PKEdgeRDD[V, E] = {
    mapEdgePartitions[V, E]((_, part) => part.filter(epred, vpred))
  }

  override def innerJoin[E2: ClassTag, E3: ClassTag](
      other: EdgeRDD[E2]
  )(f: (VertexId, VertexId, E, E2) => E3): PKEdgeRDD[V, E3] = {
    val otherRDD = other.asInstanceOf[PKEdgeRDD[V, E2]]
    val partitions = edgePartitions
      .zipPartitions(otherRDD.edgePartitions, preservesPartitioning = true) { (thisIter, otherIter) =>
        val (pid, thisEPart) = thisIter.next()
        val (_, otherEPart) = otherIter.next()
        Iterator(Tuple2(pid, thisEPart.innerJoin(otherEPart)(f)))
      }

    new PKEdgeRDD[V, E3](partitions, targetStorageLevel)
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
  ): PKEdgeRDD[V, E2] = {
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
    new PKEdgeRDD(partitions, targetStorageLevel)
  }

  /**
    * Builds a new [[PKEdgeRDD]] with the same data as this one, but using the given edge partitions.
    *
    * @param partitionsRDD New edge partitions to use
    * @tparam V2 New type of vertex attributes
    * @tparam E2 New type of edge attributes
    * @return [[PKEdgeRDD]] with given edge partitions
    */
  def withEdgePartitions[V2: ClassTag, E2: ClassTag](
      partitionsRDD: RDD[(PartitionID, PKEdgePartition[V2, E2])]
  ): PKEdgeRDD[V2, E2] = {
    new PKEdgeRDD(partitionsRDD, targetStorageLevel)
  }

  /**
    * Changes the target storage level while preserving all other properties of the
    * EdgeRDD. Operations on the returned EdgeRDD will preserve this storage level.
    *
    * This does not actually trigger a cache; to do this, call
    * [[org.apache.spark.graphx.EdgeRDD#cache]] on the returned EdgeRDD.
    */
  override def withTargetStorageLevel(level: StorageLevel): PKEdgeRDD[V, E] =
    new PKEdgeRDD(edgePartitions, level)
}

object PKEdgeRDD {

  /**
    * Creates an [[PKEdgeRDD]] from a set of edges.
    *
    * @tparam V the type of the vertex attributes that may be joined with the returned EdgeRDD
    * @tparam E the edge attribute type
    * @param edges to create RDD from
    * @param k K2Tree value
    */
  def fromEdges[V: ClassTag, E: ClassTag](
      edges: RDD[Edge[E]],
      k: Int = PKGraph.DefaultK
  ): PKEdgeRDD[V, E] = {
    val edgePartitions = edges.mapPartitionsWithIndex { (pid, iter) =>
      val builder = PKEdgePartitionBuilder[V, E](k)
      iter.foreach { e =>
        builder.add(e.srcId, e.dstId, e.attr)
      }
      Iterator((pid, builder.build))
    }
    new PKEdgeRDD(edgePartitions)
  }
}
