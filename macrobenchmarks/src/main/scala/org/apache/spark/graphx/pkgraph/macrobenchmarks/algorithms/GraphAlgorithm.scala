package org.apache.spark.graphx.pkgraph.macrobenchmarks.algorithms

import org.apache.spark.graphx.Graph

trait GraphAlgorithm {
  /**
   * Runs the graph algorithm.
   *
   * @param graph   Graph to execute algorithm on
   */
  def run(graph: Graph[Long, Int]): Unit
}

object GraphAlgorithm {
  /**
   * Get the graph algorithm implementation from the given string.
   *
   * @param algorithm     Name of the graph algorithm to use
   * @return graph algorithm implementation
   */
  def fromString(algorithm: String): GraphAlgorithm = {
    algorithm match {
      case "pageRank"            => new PageRankAlgorithm()
      case "triangleCount"       => new TriangleCountAlgorithm()
      case "connectedComponents" => new ConnectedComponentsAlgorithm()
      case "shortestPath"        => new ShortestPathAlgorithm()
      case i                     => throw new IllegalArgumentException(s"unknown algorithm '$i'")
    }
  }
}
