package org.apache.spark.graphx.pkgraph.macrobenchmarks.datasets
import org.apache.spark.SparkContext
import org.apache.spark.graphx.util.GraphGenerators

class MemoryDatasetGenerator extends GraphDatasetGenerator {
  override def dataset(sc: SparkContext): GraphDataset = {
    val graph = GraphGenerators.logNormalGraph(sc, 10000)
    GraphDataset(graph.vertices, graph.edges)
  }
}
