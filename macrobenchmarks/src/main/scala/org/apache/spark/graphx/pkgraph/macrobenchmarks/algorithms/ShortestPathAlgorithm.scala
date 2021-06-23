package org.apache.spark.graphx.pkgraph.macrobenchmarks.algorithms
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib.ShortestPaths

class ShortestPathAlgorithm extends GraphAlgorithm {
  override def run(graph: Graph[Long, Int]): Unit = {
    val v1 = graph.pickRandomVertex()
    ShortestPaths.run(graph, Seq(v1))
  }
}
