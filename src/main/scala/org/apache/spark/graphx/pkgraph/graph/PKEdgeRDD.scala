package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.graphx.impl.EdgePartition
import org.apache.spark.graphx.{EdgeRDD, PartitionID}
import org.apache.spark.rdd.RDD
import org.apache.spark.OneToOneDependency
import org.apache.spark.storage.StorageLevel

import scala.language.existentials
import scala.reflect.ClassTag

abstract class PKEdgeRDD[E: ClassTag](rdd: RDD[_]) extends EdgeRDD[E](rdd.context, List(new OneToOneDependency(rdd))) {

  /**
    * Package-private feature that is leaked in the public interface.
    * Should not be used outside of GraphX.
    *
    * @return [[NotImplementedError]]
    */
  override def partitionsRDD: RDD[(PartitionID, EdgePartition[E, V])] forSome { type V } = throw new NotImplementedError

  /**
   * Package-private feature that is leaked in the public interface.
   * Should not be used outside of GraphX.
   *
   * @return [[NotImplementedError]]
   */
  override def withTargetStorageLevel(targetStorageLevel: StorageLevel): EdgeRDD[E] = throw new NotImplementedError
}
