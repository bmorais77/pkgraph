package org.apache.spark.graphx.pkgraph.macrobenchmarks.generators

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.pkgraph.macrobenchmarks.datasets.GraphDataset

trait GraphGenerator {

  /**
    * Generates a new graph from the given dataset.
    *
    * @param dataset   Dataset containing vertices and edges of a graph
    * @return generated graph
    */
  def generate(dataset: GraphDataset): Graph[Long, Int]
}

object GraphGenerator {

  /**
    * Get the graph generator from the given implementation.
    *
    * @param implementation   Implementation name
    * @return graph generator
    */
  def fromString(implementation: String): GraphGenerator = {
    implementation match {
      case "GraphX"  => new GraphXGenerator()
      case "PKGraph2" => new PKGraphGenerator(2)
      case "PKGraph4" => new PKGraphGenerator(4)
      case "PKGraph8" => new PKGraphGenerator(8)
      case i         => throw new IllegalArgumentException(s"unknown implementation '$i'")
    }
  }
}
