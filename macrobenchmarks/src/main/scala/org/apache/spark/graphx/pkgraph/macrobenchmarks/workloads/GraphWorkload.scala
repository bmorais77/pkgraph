package org.apache.spark.graphx.pkgraph.macrobenchmarks.workloads

import org.apache.spark.graphx.Graph

trait GraphWorkload {

  /**
    * Runs the graph workload.
    *
    * @param graph   Graph to execute workload on
    */
  def run(graph: Graph[Long, Int]): Unit
}

object GraphWorkload {

  /**
    * Get the graph workload implementation from the given string.
    *
    * @param workload     Name of the graph workload to use
    * @return graph algorithm implementation
    */
  def fromString(workload: String): GraphWorkload = {
    workload match {
      case "map"                 => new MapWorkload()
      case "pageRank"            => new PageRankWorkload()
      case "triangleCount"       => new TriangleCountWorkload()
      case "connectedComponents" => new ConnectedComponentsWorkload()
      case "shortestPath"        => new ShortestPathWorkload()
      case i                     => throw new IllegalArgumentException(s"unknown algorithm '$i'")
    }
  }
}
