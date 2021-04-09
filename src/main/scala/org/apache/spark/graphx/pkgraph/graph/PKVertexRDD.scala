package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.{Dependency, SparkContext}
import org.apache.spark.graphx.{PartitionID, VertexRDD}
import org.apache.spark.graphx.impl.{ShippableVertexPartition, VertexAttributeBlock}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

abstract class PKVertexRDD[V](sc: SparkContext, deps: Seq[Dependency[_]]) extends VertexRDD[V](sc, deps) {

  /**
    * Package-private feature that is leaked in the public interface.
    * Should not be used outside of GraphX.
    *
    * @return [[NotImplementedError]]
    */
  override def partitionsRDD = throw new NotImplementedError

  /**
    * Package-private feature that is leaked in the public interface.
    * Should not be used outside of GraphX.
    *
    * @return [[NotImplementedError]]
    */
  override def mapVertexPartitions[V2: ClassTag](f: ShippableVertexPartition[V] => ShippableVertexPartition[V2]) =
    throw new NotImplementedError

  /**
    * Package-private feature that is leaked in the public interface.
    * Should not be used outside of GraphX.
    *
    * @return [[NotImplementedError]]
    */
  override def withPartitionsRDD[V2: ClassTag](partitionsRDD: RDD[ShippableVertexPartition[V2]]) =
    throw new NotImplementedError

  /**
    * Package-private feature that is leaked in the public interface.
    * Should not be used outside of GraphX.
    *
    * @return [[NotImplementedError]]
    */
  override def withTargetStorageLevel(targetStorageLevel: StorageLevel) = throw new NotImplementedError

  /**
    * Package-private feature that is leaked in the public interface.
    * Should not be used outside of GraphX.
    *
    * @return [[NotImplementedError]]
    */
  override def shipVertexAttributes(shipSrc: Boolean, shipDst: Boolean): RDD[(PartitionID, VertexAttributeBlock[V])] =
    throw new NotImplementedError

  /**
    * Package-private feature that is leaked in the public interface.
    * Should not be used outside of GraphX.
    *
    * @return [[NotImplementedError]]
    */
  override def shipVertexIds() = throw new NotImplementedError
}
