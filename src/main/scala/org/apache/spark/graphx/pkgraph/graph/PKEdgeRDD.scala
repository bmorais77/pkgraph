package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.graphx.impl.EdgePartition
import org.apache.spark.graphx.{EdgeRDD, EdgeTriplet, PartitionID, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.OneToOneDependency
import org.apache.spark.graphx.pkgraph.graph.impl.PKEdgePartition

import scala.reflect.ClassTag

abstract class PKEdgeRDD[V: ClassTag, E: ClassTag](rdd: RDD[_])
    extends EdgeRDD[E](rdd.context, List(new OneToOneDependency(rdd))) {

  /**
    * Package-private feature that is leaked in the public interface.
    * Should not be used outside of GraphX.
    *
    * @return [[NotImplementedError]]
    */
  override def partitionsRDD: RDD[(PartitionID, EdgePartition[E, V])] = throw new NotImplementedError

  /**
    * Maps the edge partition iterator according to the given user function.
    *
    * @param f User function
    * @tparam U new type for mapped iterator
    * @return new RDD with mapped type
    */
  def mapEdgePartitions[U: ClassTag](
      f: Iterator[(PartitionID, PKEdgePartition[V, E])] => Iterator[U]
  ): RDD[U]

  /**
    * Maps each edge partition according to the given user function.
    *
    * @param f User function
    * @tparam E2 new type of edge attributes
    * @return new [[PKEdgeRDD]] with mapped edge partitions
    */
  def mapEdgePartitions[E2: ClassTag](
      f: (PartitionID, PKEdgePartition[V, E]) => PKEdgePartition[V, E2]
  ): PKEdgeRDD[V, E2]

  /**
    * Filter the edge partitions according to the given vertex and edge filters.
    *
    * @param epred Edge predicate
    * @param vpred Vertex predicate
    * @return new [[PKEdgeRDD]] with filtered edge partitions
    */
  def filterPartitions(epred: EdgeTriplet[V, E] => Boolean, vpred: (VertexId, V) => Boolean): PKEdgeRDD[V, E]

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
  ): RDD[R]

  /**
    * Builds a new [[PKEdgeRDD]] with the given edge partitions
    *
    * @param partitionsRDD new edge partitions
    * @tparam V2 New type of vertex attributes
    * @tparam E2 New type of edge attributes
    * @return new [[PKEdgeRDD]] with th given edge partitions
    */
  def withEdgePartitions[V2: ClassTag, E2: ClassTag](
      partitionsRDD: RDD[(PartitionID, PKEdgePartition[V2, E2])]
  ): PKEdgeRDD[V2, E2]

  /**
    * Inner joins this EdgeRDD with another [[PKEdgeRDD]], assuming both are partitioned using the same
    * [[PartitionStrategy]].
    *
    * @param other the [[PKEdgeRDD]] to join with
    * @param f the join function applied to corresponding values of `this` and `other`
    * @return a new EdgeRDD containing only edges that appear in both `this` and `other`,
    *         with values supplied by `f`
    */
  override def innerJoin[E2: ClassTag, E3: ClassTag](other: EdgeRDD[E2])(
      f: (VertexId, VertexId, E, E2) => E3
  ): PKEdgeRDD[V, E3]
}
