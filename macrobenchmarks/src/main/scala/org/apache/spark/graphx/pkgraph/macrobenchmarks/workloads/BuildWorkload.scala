package org.apache.spark.graphx.pkgraph.macrobenchmarks.workloads
import org.apache.spark.graphx.Graph

class BuildWorkload extends GraphWorkload {
  override def run(graph: Graph[Long, Int]): Unit = {
    graph.edges.count() // Forces graph to be built
  }
}
