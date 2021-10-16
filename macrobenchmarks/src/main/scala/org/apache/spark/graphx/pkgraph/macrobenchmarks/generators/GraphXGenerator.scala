package org.apache.spark.graphx.pkgraph.macrobenchmarks.generators

import org.apache.spark.graphx.{Graph, PartitionStrategy}
import org.apache.spark.graphx.pkgraph.macrobenchmarks.datasets.GraphDataset
import org.apache.spark.storage.StorageLevel

class GraphXGenerator extends GraphGenerator {
  override def generate(dataset: GraphDataset): Graph[Long, Int] = {
    Graph(
      dataset.vertices,
      dataset.edges,
      0L,
      StorageLevel.MEMORY_AND_DISK,
      StorageLevel.MEMORY_AND_DISK
    )
  }
}
