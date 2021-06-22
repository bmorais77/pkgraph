package org.apache.spark.graphx.pkgraph.macrobenchmarks.generators
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.pkgraph.macrobenchmarks.datasets.GraphDataset

class GraphXGenerator extends GraphGenerator {
  override def generate(dataset: GraphDataset): Graph[Long, Int] = {
    Graph(dataset.vertices, dataset.edges)
  }
}
