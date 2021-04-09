package org.apache.spark.graphx.pkgraph.graph.impl

import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.pkgraph.util.SparkSessionTestWrapper
import org.apache.spark.storage.StorageLevel
import org.scalatest.FlatSpec

class PKGraphImplSpec extends FlatSpec with SparkSessionTestWrapper {
  private val vertices = (0 until 10).map(i => (i.toLong, i * 20))
  private val edges = (0 until 10).map(i => Edge(i, i, i * 10))

  "A PKGraphImpl" should "build from edges" in {
    val edges = sc.parallelize((0 until 10).map(i => Edge(i, i, i * 10)))
    val graph = PKGraphImpl(edges, 0, StorageLevel.MEMORY_ONLY, StorageLevel.MEMORY_ONLY)
    assert(graph.numEdges == edges.count())
  }

  it should "build from edges and vertices" in {
    val edges = sc.parallelize((0 until 10).map(i => Edge(i, i, i * 10)))
    val vertices = sc.parallelize((0 until 10).map(i => (i.toLong, i * 20)))
    val graph = PKGraphImpl(vertices, edges, 0, StorageLevel.MEMORY_ONLY, StorageLevel.MEMORY_ONLY)
    assert(graph.numEdges == edges.count())
    assert(graph.numVertices == vertices.count())
  }

  it should "collect edge triplets" in {
    val graph = buildGraph()
    val triplets = graph.triplets.collect()
    assert(triplets.length == graph.numEdges)

    var i = 0
    for(triplet <- triplets) {
      assert(triplet.srcId == i)
      assert(triplet.dstId == i)
      assert(triplet.attr == i * 10)
      assert(triplet.srcAttr == i * 20)
      assert(triplet.srcAttr == triplet.dstAttr)
      i += 1
    }
  }

  it should "persist vertices and edges" in {
    buildGraph().persist(StorageLevel.DISK_ONLY)
  }

  it should "cache vertices and edges" in {
    buildGraph().cache()
  }

  it should "checkpoint vertices and edges" in {
    val graph = buildGraph()
    graph.checkpoint()
    assert(graph.isCheckpointed)
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

  it should "reverse edges" in {
    val graph = buildGraph()
    val newGraph = graph.reverse

    val actualEdges = newGraph.edges.collect().sortWith((a, b) => a.attr < b.attr)
    val expectedEdges = edges.toArray.sortWith((a, b) => a.attr < b.attr)
    assert(actualEdges sameElements expectedEdges)
  }

  private def buildGraph(): PKGraphImpl[Int, Int] = {
    val edgeRDD = sc.parallelize(edges)
    val vertexRDD = sc.parallelize(vertices)
    PKGraphImpl(vertexRDD, edgeRDD, 0, StorageLevel.MEMORY_ONLY, StorageLevel.MEMORY_ONLY)
  }
}
