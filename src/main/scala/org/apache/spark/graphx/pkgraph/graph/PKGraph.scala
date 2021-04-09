package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.graphx.pkgraph.graph.impl.PKGraphImpl
import org.apache.spark.graphx.{Edge, EdgeContext, EdgeDirection, Graph, GraphOps, TripletFields, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

abstract class PKGraph[V: ClassTag, E: ClassTag] extends Graph[V, E] {

  /**
    * Package-private feature that is leaked in the public interface.
    * Should not be used outside of GraphX.
    *
    * @return [[NotImplementedError]]
    */
  override def aggregateMessagesWithActiveSet[A: ClassTag](
      sendMsg: EdgeContext[V, E, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields,
      activeSetOpt: Option[(VertexRDD[_], EdgeDirection)]
  ): VertexRDD[A] = throw new NotImplementedError
}

object PKGraph {

  /**
    * Construct a graph from a collection of edges encoded as vertex id pairs.
    *
    * @param rawEdges a collection of edges in (src, dst) form
    * @param defaultValue the vertex attributes with which to create vertices referenced by the edges
    * @param edgeStorageLevel the desired storage level at which to cache the edges if necessary
    * @param vertexStorageLevel the desired storage level at which to cache the vertices if necessary
    *
    * @return a graph with edge attributes containing either the count of duplicate edges or 1
    * (if `uniqueEdges` is `None`) and vertex attributes containing the total degree of each vertex.
    */
  def fromEdgeTuples[V: ClassTag](
      rawEdges: RDD[(VertexId, VertexId)],
      defaultValue: V,
      edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
  ): PKGraph[V, Int] = {
    // TODO: Could add a function here to merge the attributes of identical edges
    val edges = rawEdges.map(p => Edge(p._1, p._2, 1))
    PKGraphImpl(edges, defaultValue, edgeStorageLevel, vertexStorageLevel)
  }

  /**
    * Construct a graph from a collection of edges.
    *
    * @param edges the RDD containing the set of edges in the graph
    * @param defaultValue the default vertex attribute to use for each vertex
    * @param edgeStorageLevel the desired storage level at which to cache the edges if necessary
    * @param vertexStorageLevel the desired storage level at which to cache the vertices if necessary
    *
    * @return a graph with edge attributes described by `edges` and vertices
    *         given by all vertices in `edges` with value `defaultValue`
    */
  def fromEdges[V: ClassTag, E: ClassTag](
      edges: RDD[Edge[E]],
      defaultValue: V,
      edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
  ): PKGraph[V, E] = {
    PKGraphImpl(edges, defaultValue, edgeStorageLevel, vertexStorageLevel)
  }

  /**
    * Construct a graph from a collection of vertices and
    * edges with attributes.  Duplicate vertices are picked arbitrarily and
    * vertices found in the edge collection but not in the input
    * vertices are assigned the default attribute.
    *
    * @tparam V the vertex attribute type
    * @tparam E the edge attribute type
    * @param vertices the "set" of vertices and their attributes
    * @param edges the collection of edges in the graph
    * @param defaultVertexAttr the default vertex attribute to use for vertices that are
    *                          mentioned in edges but not in vertices
    * @param edgeStorageLevel the desired storage level at which to cache the edges if necessary
    * @param vertexStorageLevel the desired storage level at which to cache the vertices if necessary
    */
  def apply[V: ClassTag, E: ClassTag](
      vertices: RDD[(VertexId, V)],
      edges: RDD[Edge[E]],
      defaultVertexAttr: V = null.asInstanceOf[V],
      edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
  ): PKGraph[V, E] = {
    PKGraphImpl(vertices, edges, defaultVertexAttr, edgeStorageLevel, vertexStorageLevel)
  }

  /**
    * Implicitly extracts the [[GraphOps]] member from a graph.
    *
    * To improve modularity the Graph type only contains a small set of basic operations.
    * All the convenience operations are defined in the [[GraphOps]] class which may be
    * shared across multiple graph implementations.
    */
  implicit def graphToGraphOps[V: ClassTag, E: ClassTag](g: PKGraph[V, E]): GraphOps[V, E] = g.ops
}
