package org.apache.spark.graphx.pkgraph.macrobenchmarks.generators

import org.apache.spark.graphx.{Graph, PartitionStrategy}
import org.apache.spark.graphx.pkgraph.macrobenchmarks.datasets.GraphDataset
import org.apache.spark.storage.StorageLevel

class GraphXGenerator extends GraphGenerator {
  override def generate(dataset: GraphDataset, partitionCount: Int): Graph[Long, Int] = {
    val graph = Graph(
      dataset.vertices,
      dataset.edges,
      0L,
      StorageLevel.MEMORY_AND_DISK,
      StorageLevel.MEMORY_AND_DISK
    )

    if(partitionCount > 0) {
      graph.partitionBy(PartitionStrategy.EdgePartition2D, partitionCount)
    } else {
      graph
    }
  }
}
