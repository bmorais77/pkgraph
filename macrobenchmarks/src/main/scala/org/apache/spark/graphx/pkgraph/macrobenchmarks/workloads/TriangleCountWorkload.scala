package org.apache.spark.graphx.pkgraph.macrobenchmarks.workloads
import org.apache.spark.graphx.Graph

class TriangleCountWorkload extends GraphWorkload {
  override def run(graph: Graph[Long, Int]): Unit = {
    graph.triangleCount().edges.count()
  }
}
