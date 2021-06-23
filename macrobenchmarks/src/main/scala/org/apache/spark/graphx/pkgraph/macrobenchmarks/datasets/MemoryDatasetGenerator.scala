package org.apache.spark.graphx.pkgraph.macrobenchmarks.datasets
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.util.GraphGenerators

class MemoryDatasetGenerator extends GraphDatasetGenerator {
  override def dataset(sc: SparkContext): GraphDataset = {
    val vertices = sc.parallelize((0 until 10000).map(i => (i.toLong, i * 10L)))
    val edges = sc.parallelize((0 until 2500).map(i => Edge(i, i, i * 20)))
    GraphDataset(vertices, edges)
  }
}
