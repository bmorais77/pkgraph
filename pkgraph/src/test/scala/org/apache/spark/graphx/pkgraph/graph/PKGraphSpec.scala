package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.graphx.pkgraph.SparkSessionTestWrapper
import org.apache.spark.graphx.{Edge, VertexId}
import org.scalatest.FlatSpec

class PKGraphSpec extends FlatSpec with SparkSessionTestWrapper {
  private val vertices = (0 until 10).map(i => (i.toLong, i * 20))
  private val edges = (0 until 10).map(i => Edge(i, i, i * 10))

  "A PKGraph" should "build from edges" in {
    val edges = sc.parallelize((0 until 10).map(i => Edge(i, i, i * 10)))
    val graph = PKGraph.fromEdges(2, edges, 0)
    assert(graph.numEdges == edges.count())
  }

  it should "build from edges and vertices" in {
    val edges = sc.parallelize((0 until 10).map(i => Edge(i, i, i * 10)))
    val vertices = sc.parallelize((0 until 10).map(i => (i.toLong, i * 20)))
    val graph = PKGraph(2, vertices, edges, 0)
    assert(graph.numEdges == edges.count())
    assert(graph.numVertices == vertices.count())
  }

  it should "build from large number of edges and vertices" in {
    val size = 50000
    val vertices = sc.parallelize((0 until size).map(i => (i.toLong, i * 10)))
    val edges = sc.parallelize((0 until math.floor(math.sqrt(size)).toInt).map(i => Edge(i.toLong, i.toLong, i * 20)))
    val graph = PKGraph(2, vertices, edges)
    assert(graph.numEdges == edges.count())
    assert(graph.numVertices == vertices.count())
  }

  it should "collect edge triplets" in {
    val graph = buildGraph()
    val triplets = graph.triplets.collect()
    assert(triplets.length == graph.numEdges)

    var i = 0
    for (triplet <- triplets) {
      assert(triplet.srcId == i)
      assert(triplet.dstId == i)
      assert(triplet.attr == i * 10)
      assert(triplet.srcAttr == i * 20)
      assert(triplet.srcAttr == triplet.dstAttr)
      i += 1
    }
  }

  it should "map vertices" in {
    val graph = buildGraph()
    val newGraph = graph.mapVertices((_, attr) => attr * 2)

    val actualVertices = newGraph.vertices.collect().sortWith((a, b) => a._1 < b._1)
    val expectedVertices = vertices.map(v => (v._1, v._2 * 2)).toArray.sortWith((a, b) => a._1 < b._1)
    assert(actualVertices sameElements expectedVertices)
  }

  it should "map edges" in {
    val graph = buildGraph()
    val newGraph = graph.mapEdges(_.attr * 2)

    val actualEdges = newGraph.edges.collect().sortWith((a, b) => a.attr < b.attr)
    val expectedEdges = edges.map(e => e.copy(attr = e.attr * 2)).toArray.sortWith((a, b) => a.attr < b.attr)
    assert(actualEdges sameElements expectedEdges)
  }

  it should "map triplets" in {
    val graph = buildGraph()
    val newGraph = graph.mapTriplets(e => e.attr * 2)

    val actualEdges = newGraph.edges.collect().sortWith((a, b) => a.attr < b.attr)
    val expectedEdges = edges.map(e => e.copy(attr = e.attr * 2)).toArray.sortWith((a, b) => a.attr < b.attr)
    assert(actualEdges sameElements expectedEdges)
  }

  it should "reverse edges" in {
    val graph = buildGraph()
    val newGraph = graph.reverse

    val actualEdges = newGraph.edges.collect().sortWith((a, b) => a.attr < b.attr)
    val expectedEdges = edges.toArray.sortWith((a, b) => a.attr < b.attr)
    assert(actualEdges sameElements expectedEdges)
  }

  it should "subgraph original graph" in {
    val graph = buildGraph()
    val newGraph = graph.subgraph(_.attr % 2 == 0)

    val actualEdges = newGraph.edges.collect().sortWith((a, b) => a.attr < b.attr)
    val expectedEdges = edges.filter(_.attr % 2 == 0).toArray.sortWith((a, b) => a.attr < b.attr)
    assert(actualEdges sameElements expectedEdges)
  }

  it should "inner join with another graph" in {
    val edges1 = (0 to 6).map(i => Edge(i, i, i))
    val edges2 = (4 until 9).map(i => Edge(i, i, i))

    val graph1 = buildGraph(edges1)
    val graph2 = buildGraph(edges2)
    val newGraph = graph1.mask(graph2)

    val actualEdges = newGraph.edges.collect().sortWith((a, b) => a.attr < b.attr)
    val expectedEdges = (4 to 6).map(i => Edge(i, i, i)).sortWith((a, b) => a.attr < b.attr).toArray
    assert(actualEdges sameElements expectedEdges)
  }

  it should "left outer join vertices" in {
    val newVertices = sc.parallelize((5 until 10).map(i => (i.toLong, i * 20)))

    val graph = buildGraph()
    val newGraph = graph.outerJoinVertices(newVertices) { (_, attr, other) => attr + other.getOrElse(0) }

    val actualVertices = newGraph.vertices.collect().sortWith((a, b) => a._1 < b._1)
    val expectedVertices = (0 until 10)
      .map(i => {
        if (i >= 5) {
          (i.toLong, i * 20 * 2)
        } else {
          (i.toLong, i * 20)
        }
      })
      .toArray
    assert(actualVertices sameElements expectedVertices)
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
