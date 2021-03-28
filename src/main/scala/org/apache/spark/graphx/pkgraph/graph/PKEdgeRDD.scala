package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.graphx.impl.EdgePartition
import org.apache.spark.graphx.{EdgeRDD, PartitionID, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.OneToOneDependency

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
    * Inner joins this EdgeRDD with another [[PKEdgeRDD]], assuming both are partitioned using the same
    * [[PartitionStrategy]].
    *
    * @param other the [[PKEdgeRDD]] to join with
    * @param f the join function applied to corresponding values of `this` and `other`
    * @tparam E2 Type of edge attribute of the RDD to join with
    * @tparam E3 Resulting type of edge attribute
    * @return a new EdgeRDD containing only edges that appear in both `this` and `other`,
    *         with values supplied by `f`
    */
  override def innerJoin[E2: ClassTag, E3: ClassTag](other: EdgeRDD[E2])(
      f: (VertexId, VertexId, E, E2) => E3
  ): PKEdgeRDD[E3]
}
