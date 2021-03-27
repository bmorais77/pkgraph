package org.apache.spark.graphx.pkgraph.graph.impl

import org.apache.spark.graphx.pkgraph.graph.PKVertexRDD
import org.apache.spark.graphx.{EdgeRDD, PartitionID, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

private[pkgraph] class PKVertexRDDImpl[V](val rdd: VertexRDD[V])(implicit
    override protected val vdTag: ClassTag[V]
) extends PKVertexRDD[V](rdd.context, rdd.dependencies) {

  /**
    * Prepare the vertex attributes in this [[PKVertexRDD]] to be shipped to an edge partition.
    *
    * @param shipSrc Include source vertex attributes
    * @param shipDst Include destination vertex attributes
    * @return RDD with shipped attributes
    */
  override def shipAttributes(shipSrc: Boolean, shipDst: Boolean): RDD[(PartitionID, B)] forSome { type B } =
    rdd.shipVertexAttributes(shipSrc, shipDst)

  /**
    * Filters all vertices according to the given user predicate.
    *
    * @param pred User predicate
    * @return new [[PKVertexRDD]] with only vertices that passed the predicate
    */
  override def filterVertices(pred: (VertexId, V) => Boolean): PKVertexRDD[V] =
    new PKVertexRDDImpl(rdd.filter(Function.tupled(pred)))

  // TODO: Implement this without using partitionsRDD from PKEdgeRDD
  override def withEdges(edges: EdgeRDD[_]): PKVertexRDDImpl[V] = new PKVertexRDDImpl(rdd.withEdges(edges))

  override def reverseRoutingTables(): PKVertexRDD[V] = new PKVertexRDDImpl(rdd.reverseRoutingTables())

  override def reindex(): PKVertexRDD[V] = new PKVertexRDDImpl(rdd.reindex())

  override def mapValues[VD2: ClassTag](f: V => VD2): PKVertexRDD[VD2] =
    new PKVertexRDDImpl(rdd.mapValues(f))

  override def mapValues[VD2: ClassTag](f: (VertexId, V) => VD2): PKVertexRDD[VD2] =
    new PKVertexRDDImpl(rdd.mapValues(f))

  override def minus(other: RDD[(VertexId, V)]): PKVertexRDD[V] =
    new PKVertexRDDImpl(rdd.minus(other))

  override def minus(other: VertexRDD[V]): PKVertexRDD[V] =
    new PKVertexRDDImpl(rdd.minus(other))

  override def diff(other: RDD[(VertexId, V)]): PKVertexRDD[V] =
    new PKVertexRDDImpl(rdd.diff(other))

  override def diff(other: VertexRDD[V]): PKVertexRDD[V] =
    new PKVertexRDDImpl(rdd.diff(other))

  override def leftZipJoin[V2: ClassTag, V3: ClassTag](other: VertexRDD[V2])(
      f: (VertexId, V, Option[V2]) => V3
  ): PKVertexRDD[V3] = new PKVertexRDDImpl(rdd.leftZipJoin(other)(f))

  override def leftJoin[V2: ClassTag, V3: ClassTag](other: RDD[(VertexId, V2)])(
      f: (VertexId, V, Option[V2]) => V3
  ): PKVertexRDD[V3] = new PKVertexRDDImpl(rdd.leftJoin(other)(f))

  override def innerZipJoin[U: ClassTag, V2: ClassTag](other: VertexRDD[U])(
      f: (VertexId, V, U) => V2
  ): PKVertexRDD[V2] = new PKVertexRDDImpl(rdd.innerZipJoin(other)(f))

  override def innerJoin[U: ClassTag, V2: ClassTag](other: RDD[(VertexId, U)])(
      f: (VertexId, V, U) => V2
  ): PKVertexRDD[V2] = new PKVertexRDDImpl(rdd.innerJoin(other)(f))

  override def aggregateUsingIndex[V2: ClassTag](
      messages: RDD[(VertexId, V2)],
      reduceFunc: (V2, V2) => V2
  ): PKVertexRDD[V2] = new PKVertexRDDImpl(rdd.aggregateUsingIndex(messages, reduceFunc))
}
