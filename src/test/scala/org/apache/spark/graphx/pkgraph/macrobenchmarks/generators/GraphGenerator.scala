package org.apache.spark.graphx.pkgraph.macrobenchmarks.generators

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.pkgraph.macrobenchmarks.datasets.GraphDataset

trait GraphGenerator {
  def generate(dataset: GraphDataset): Graph[Long, Int]
}
