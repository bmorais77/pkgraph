package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

/**
 * Dynamic graph interface containing dynamic operations.
 *
 * @tparam V    Type of vertex attributes
 * @tparam E    Type of edge attributes
 */
trait DynamicGraph[V, E]{
  /**
   * Creates a new graph by adding the given vertices to this graph.
   *
   * @param other   Vertices to add
   * @return new graph with added vertices
   */
  def addVertices(other: RDD[(VertexId, V)]): Graph[V, E]

  /**
   * Creates a new graph by adding the given edges to this graph.
   *
   * @param other   Edges to add
   * @return new graph with added edges
   */
  def addEdges(other: RDD[Edge[E]]): Graph[V, E]

  /**
   * Creates a new graph by removing the given vertices from this graph.
   *
   * @param other   Vertex identifiers to remove
   * @return new graph with removed vertices
   */
  def removeVertices(other: RDD[VertexId]): Graph[V, E]

  /**
   * Creates a new graph by removing the given vertices from this graph.
   *
   * @param other   Edge identifiers to remove
   * @return new graph with removed edges
   */
  def removeEdges(other: RDD[(VertexId, VertexId)]): Graph[V, E]
}
