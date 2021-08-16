package org.apache.spark.graphx.pkgraph.macrobenchmarks.workloads
import org.apache.spark.graphx.Graph

class ConnectedComponentsWorkload extends GraphWorkload {
  override def run(graph: Graph[Long, Int]): Unit = {
    graph.connectedComponents()
  }
}
