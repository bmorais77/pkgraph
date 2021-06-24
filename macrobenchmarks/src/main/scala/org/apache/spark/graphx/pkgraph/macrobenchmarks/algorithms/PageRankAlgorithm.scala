package org.apache.spark.graphx.pkgraph.macrobenchmarks.algorithms
import org.apache.spark.graphx.{Graph, PartitionStrategy}

class PageRankAlgorithm extends GraphAlgorithm {
  override def run(graph: Graph[Long, Int]): Unit = {
    graph
      .partitionBy(PartitionStrategy.EdgePartition2D, 100)
      .pageRank(0.1)
  }
}
