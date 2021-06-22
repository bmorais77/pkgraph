package org.apache.spark.graphx.pkgraph.macrobenchmarks.algorithms
import org.apache.spark.graphx.Graph

class PageRankAlgorithm extends GraphAlgorithm {
  override def run(graph: Graph[Long, Int]): Unit = {
    graph.pageRank(0.1)
  }
}
