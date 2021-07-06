package org.apache.spark.graphx.pkgraph.macrobenchmarks.datasets

import org.apache.spark.SparkContext

trait GraphDatasetReader {
  def readDataset(sc: SparkContext, path: String): GraphDataset
}
