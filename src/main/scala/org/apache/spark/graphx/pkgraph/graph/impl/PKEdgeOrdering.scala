package org.apache.spark.graphx.pkgraph.graph.impl

import org.apache.spark.graphx.Edge

class PKEdgeOrdering[E] extends Ordering[Edge[E]] {
  override def compare(a: Edge[E], b: Edge[E]): Int = {
    if(a.srcId < b.srcId || a.srcId == b.srcId && a.dstId < b.dstId) {
      -1
    } else if(a.srcId == b.srcId && a.dstId == b.dstId) {
      0
    } else {
      1
    }
  }
}
