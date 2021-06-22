package org.apache.spark.graphx.pkgraph.macrobenchmarks.algorithms

import org.apache.spark.graphx.Graph

trait GraphAlgorithm {
  def run(graph: Graph[Long, Int]): Unit
}
