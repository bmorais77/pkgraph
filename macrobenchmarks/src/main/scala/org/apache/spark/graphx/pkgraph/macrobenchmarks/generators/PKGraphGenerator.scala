package org.apache.spark.graphx.pkgraph.macrobenchmarks.generators

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.pkgraph.graph.PKGraph
import org.apache.spark.graphx.pkgraph.macrobenchmarks.datasets.GraphDataset
import org.apache.spark.storage.StorageLevel

class PKGraphGenerator(k: Int) extends GraphGenerator {
  override def generate(dataset: GraphDataset): Graph[Long, Int] = {
    PKGraph(
      k,
      dataset.vertices,
      dataset.edges,
      0L,
      StorageLevel.MEMORY_AND_DISK,
      StorageLevel.MEMORY_AND_DISK
    )
  }
}
