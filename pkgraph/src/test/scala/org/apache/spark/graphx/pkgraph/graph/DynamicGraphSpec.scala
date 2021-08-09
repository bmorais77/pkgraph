package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.graphx.pkgraph.SparkSessionTestWrapper
import org.scalatest.FlatSpec

class DynamicGraphSpec extends FlatSpec with SparkSessionTestWrapper {
  private val vertices = (0 until 10).map(i => (i.toLong, i * 20))
  private val edges = (0 until 10).map(i => Edge(i, i, i * 10))

  "A Dynamic Graph" should "add new vertices" in {
    val newVertices = sc.parallelize((10 until 20).map(i => (i.toLong, i * 20)))

    val graph = buildGraph()
    val newGraph = graph.addVertices(newVertices)

    val actualVertices = newGraph.vertices.collect().sortWith((a, b) => a._1 < b._1)
    val expectedVertices = (0 until 20).map(i => (i.toLong, i * 20)).toArray
    assert(expectedVertices sameElements actualVertices)
  }

  it should "add new edges" in {
    val newEdges = sc.parallelize((10 until 20).map(i => Edge(i, i, i * 10)))

    val graph = buildGraph()
    val newGraph = graph.addEdges(newEdges)

    val actualEdges = newGraph.edges.collect().sortWith((a, b) => a.srcId < b.srcId)
    val expectedEdges = (0 until 20).map(i => Edge(i, i, i * 10)).toArray
    assert(expectedEdges sameElements actualEdges)
  }

  it should "remove existing vertices" in {
    val removedVertices = sc.parallelize(0L until 5L)

    val graph = buildGraph()
    val newGraph = graph.removeVertices(removedVertices)

    val actualVertices = newGraph.vertices.collect().sortWith((a, b) => a._1 < b._1)
    val expectedVertices = (5 until 10).map(i => (i.toLong, i * 20)).toArray
    assert(expectedVertices sameElements actualVertices)
  }

  it should "remove existing edges" in {
    val removedEdges = sc.parallelize((0L until 5L).map(i => (i, i)))

    val graph = buildGraph()
    val newGraph = graph.removeEdges(removedEdges)

    val actualEdges = newGraph.edges.collect().sortWith((a, b) => a.srcId < b.srcId)
    val expectedEdges = (5 until 10).map(i => Edge(i, i, i * 10)).toArray
    assert(expectedEdges sameElements actualEdges)
  }

  private def buildGraph(
      edges: Seq[Edge[Int]] = edges,
      vertices: Seq[(VertexId, Int)] = vertices
  ): PKGraph[Int, Int] = {
    val edgeRDD = sc.parallelize(edges)
    val vertexRDD = sc.parallelize(vertices)
    PKGraph(2, vertexRDD, edgeRDD, 0)
  }
}
