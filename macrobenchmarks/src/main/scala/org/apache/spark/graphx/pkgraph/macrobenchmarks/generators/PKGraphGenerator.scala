package org.apache.spark.graphx.pkgraph.macrobenchmarks.generators
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.pkgraph.graph.PKGraph
import org.apache.spark.graphx.pkgraph.macrobenchmarks.datasets.GraphDataset

class PKGraphGenerator(k: Int) extends GraphGenerator {
  override def generate(dataset: GraphDataset): Graph[Long, Int] = {
    PKGraph(k, dataset.vertices, dataset.edges)
  }
}
