package org.apache.spark.graphx.pkgraph.macrobenchmarks.workloads
import org.apache.spark.graphx.Graph

class MapWorkload extends GraphWorkload {
  override def run(graph: Graph[Long, Int]): Unit = {
    graph.mapEdges(_.attr * 10).edges.count() // Forces graph to iterate edges
  }
}
