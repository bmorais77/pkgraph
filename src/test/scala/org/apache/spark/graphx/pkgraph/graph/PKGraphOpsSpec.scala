package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.graphx.pkgraph.util.SparkSessionTestWrapper
import org.apache.spark.graphx.{Edge, EdgeDirection, VertexId}
import org.scalatest.FlatSpec

class PKGraphOpsSpec extends FlatSpec with SparkSessionTestWrapper {
  private val vertices = (0 until 10).map(i => (i.toLong, i * 20))
  private val edges = (0 until 10).map(i => Edge(i, i, i * 10))
  private val ops = buildGraph().ops

  "A PKGraphOps" should "get the number of edges" in {
    assert(ops.numEdges == edges.length)
  }

  it should "get the number of vertices" in {
    assert(ops.numVertices == vertices.length)
  }

  it should "get in-degree of each vertex" in {
    val degrees = ops.inDegrees.collect()
    assert(degrees.length == vertices.length)
    assert(degrees.forall(_._2 == 1))
  }

  it should "get out-degree of each vertex" in {
    val degrees = ops.outDegrees.collect()
    assert(degrees.length == vertices.length)
    assert(degrees.forall(_._2 == 1))
  }

  it should "get degree of each vertex" in {
    val degrees = ops.degrees.collect()
    assert(degrees.length == vertices.length)
    assert(degrees.forall(_._2 == 2))
  }

  it should "collect neighbor IDs" in {
    val neighbors = ops.collectNeighborIds(EdgeDirection.In).collect()
    assert(neighbors.length == vertices.length)
    assert(neighbors.forall(_._2.length == 1))
  }

  it should "collect neighbors" in {
    val neighbors = ops.collectNeighbors(EdgeDirection.In).collect()
    assert(neighbors.length == vertices.length)
    assert(neighbors.forall(_._2.length == 1))
  }

  it should "collect edges" in {
    val edges = ops.collectEdges(EdgeDirection.In).collect()
    assert(edges.length == vertices.length)
    assert(edges.forall(_._2.length == 1))
  }

  it should "remove self edges" in {
    val newGraph = ops.removeSelfEdges()
    assert(newGraph.numEdges == 0)
  }

  it should "join vertices" in {
    val newVertices = sc.parallelize(Seq((1L, 20), (2L, 40)))
    val newGraph = ops.joinVertices(newVertices) { (_, attr, other) => attr + other }

    assert(newGraph.numVertices == vertices.length)

    val expectedVertices = (0 until 10)
      .map(i => {
        if (i > 2) {
          (i.toLong, i * 20)
        } else {
          (i.toLong, i * 20 * 2)
        }
      })
      .toArray
    val actualVertices = newGraph.vertices.collect().sortWith((a, b) => a._1 < b._1)
    assert(expectedVertices sameElements actualVertices)
  }

  it should "filter a graph" in {
    val newGraph = ops.filter[Int, Int](preprocess = g => g, epred = _.attr % 2 == 0)
    val expectedEdges = edges.filter(_.attr % 2 == 0).toArray.sortWith((a, b) => a.attr < b.attr)
    val actualEdges = newGraph.edges.collect().sortWith((a, b) => a.attr < b.attr)
    assert(expectedEdges sameElements actualEdges)
  }

  it should "pick a random vertex" in {
    val vertexId = ops.pickRandomVertex()
    assert(vertexId >= 0 && vertexId < 10)
  }

  it should "perform page rank" in {
    val newGraph = ops.pageRank(0.001)
    val expectedVertices = (0 until 10).map(i => (i.toLong, 1)).toArray
    val actualVertices = newGraph.vertices.collect().sortWith((a, b) => a._1 < b._1).map(v => (v._1, math.round(v._2)))
    assert(expectedVertices sameElements actualVertices)
  }

  it should "perform static page rank" in {
    val newGraph = ops.staticPageRank(2)
    val expectedVertices = (0 until 10).map(i => (i.toLong, 1)).toArray
    val actualVertices = newGraph.vertices.collect().sortWith((a, b) => a._1 < b._1).map(v => (v._1, math.round(v._2)))
    assert(expectedVertices sameElements actualVertices)
  }

  it should "perform connect components" in {
    val newGraph = ops.connectedComponents(2)
    val expectedVertices = (0 until 10).map(i => (i.toLong, i)).toArray
    val actualVertices = newGraph.vertices.collect().sortWith((a, b) => a._1 < b._1)
    assert(expectedVertices sameElements actualVertices)
  }

  it should "perform triangle count" in {
    val newGraph = ops.triangleCount()
    val expectedVertices = (0 until 10).map(i => (i.toLong, 0)).toArray
    val actualVertices = newGraph.vertices.collect().sortWith((a, b) => a._1 < b._1)
    assert(expectedVertices sameElements actualVertices)
  }

  it should "perform strongly connected components" in {
    val newGraph = ops.stronglyConnectedComponents(2)
    val expectedVertices = (0 until 10).map(i => (i.toLong, i)).toArray
    val actualVertices = newGraph.vertices.collect().sortWith((a, b) => a._1 < b._1)
    assert(expectedVertices sameElements actualVertices)
  }

  private def buildGraph(
      edges: Seq[Edge[Int]] = edges,
      vertices: Seq[(VertexId, Int)] = vertices
  ): PKGraph[Int, Int] = {
    val edgeRDD = sc.parallelize(edges)
    val vertexRDD = sc.parallelize(vertices)
    PKGraph(vertexRDD, edgeRDD)
  }
}
