package pt.tecnico.ulisboa.meic.graph.impl

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, OneToOneDependency, Partition, Partitioner}

import scala.reflect.ClassTag

class PKEdgeRDD[V: ClassTag, E: ClassTag] private[graph] (
    val edgePartitions: RDD[(PartitionID, PKEdgePartition[V, E])],
    val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
) extends EdgeRDD[E](edgePartitions.context, List(new OneToOneDependency(edgePartitions))) {

  override def setName(_name: String): this.type = {
    if (edgePartitions.name != null) {
      edgePartitions.setName(edgePartitions.name + ", " + _name)
    } else {
      edgePartitions.setName(_name)
    }
    this
  }
  setName("EdgeRDD")

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

  def mapValues[E2: ClassTag](f: Edge[E] => E2): PKEdgeRDD[V, E2] = mapEdgePartitions((_, part) => part.map(f))

  def reverse: PKEdgeRDD[V, E] = mapEdgePartitions((_, part) => part.reverse)

  def filter(epred: EdgeTriplet[V, E] => Boolean, vpred: (VertexId, V) => Boolean): PKEdgeRDD[V, E] = {
    mapEdgePartitions((_, part) => part.filter(epred, vpred))
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

  def mapEdgePartitions[V2: ClassTag, E2: ClassTag](
      f: (PartitionID, PKEdgePartition[V, E]) => PKEdgePartition[V2, E2]
  ): PKEdgeRDD[V2, E2] = {
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

  def withEdgePartitions[V2: ClassTag, E2: ClassTag](
      partitionsRDD: RDD[(PartitionID, PKEdgePartition[V2, E2])]
  ): PKEdgeRDD[V2, E2] = {
    new PKEdgeRDD(partitionsRDD, targetStorageLevel)
  }

  override def withTargetStorageLevel(targetStorageLevel: StorageLevel): PKEdgeRDD[V, E] = {
    new PKEdgeRDD(edgePartitions, targetStorageLevel)
  }
}
