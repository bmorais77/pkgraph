package org.apache.spark.graphx.pkgraph.graph.impl

import org.apache.spark.graphx._
import org.apache.spark.graphx.pkgraph.graph.PKEdgeRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, Partition, Partitioner}

import scala.reflect.ClassTag

private[graph] class PKEdgeRDDImpl[V: ClassTag, E: ClassTag](
    val edgePartitions: RDD[(PartitionID, PKEdgePartition[V, E])],
    val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
) extends PKEdgeRDD[E](edgePartitions) {

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

  override def collect(): Array[Edge[E]] = this.map(_.copy()).collect()

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

  override def partitionsRDD = throw new NotImplementedError

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
    * Maps each edge partition according to the given user function.
    *
    * @param f User function
    * @tparam U new type for mapped iterator
    * @return new RDD with mapped type
    */
  def mapEdgePartitions[U: ClassTag](
      f: Iterator[(PartitionID, PKEdgePartition[V, E])] => Iterator[U]
  ): RDD[U] = ???

  /**
    * Filter the edge partitions according to the given vertex and edge filters.
    *
    * @param epred Edge predicate
    * @param vpred Vertex predicate
    * @return new [[PKEdgeRDD]] with filtered edge partitions
    */
  def filterPartitions(epred: EdgeTriplet[V, E] => Boolean, vpred: (VertexId, V) => Boolean): PKEdgeRDDImpl[V, E] =
    ???

  /**
    * Zips the `other` RDD with this one according to the given user function.
    *
    * @param other Other RDD to zip
    * @param f User function
    * @tparam U Type of data in `other` RDD
    * @tparam R Type of data in result of zip
    * @return RDD with edge partitions zipped with `other` RDD
    */
  def zipEdgePartitions[U: ClassTag, R: ClassTag](other: RDD[U])(
      f: (Iterator[(PartitionID, PKEdgePartition[V, E])], Iterator[U]) => Iterator[R]
  ): RDD[R] = ???

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
