package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.graphx.{Edge, Graph, GraphOps}
import spire.ClassTag

class PKGraphOps[V: ClassTag, E: ClassTag](graph: Graph[V, E]) extends GraphOps[V, E](graph) {
  override def convertToCanonicalEdges(mergeFunc: (E, E) => E): Graph[V, E] = {
    val newEdges =
      graph.edges
        .map {
          case e if e.srcId < e.dstId => ((e.srcId, e.dstId), e.attr)
          case e                      => ((e.dstId, e.srcId), e.attr)
        }
        .reduceByKey(mergeFunc)
        .map(e => new Edge(e._1._1, e._1._2, e._2))
    PKGraph(graph.vertices, newEdges)
  }
}
