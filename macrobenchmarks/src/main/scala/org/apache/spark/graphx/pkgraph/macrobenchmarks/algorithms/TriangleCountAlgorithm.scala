package org.apache.spark.graphx.pkgraph.macrobenchmarks.algorithms
import org.apache.spark.graphx.Graph

class TriangleCountAlgorithm extends GraphAlgorithm {
  override def run(graph: Graph[Long, Int]): Unit = {
    graph.triangleCount().edges.count()
  }
}
