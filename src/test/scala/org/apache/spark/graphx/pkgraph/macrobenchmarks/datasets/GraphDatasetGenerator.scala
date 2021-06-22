package org.apache.spark.graphx.pkgraph.macrobenchmarks.datasets

import org.apache.spark.SparkContext

abstract class GraphDatasetGenerator {
  def dataset(sc: SparkContext): GraphDataset
}
