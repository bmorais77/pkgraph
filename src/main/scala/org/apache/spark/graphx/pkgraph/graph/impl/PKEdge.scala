package org.apache.spark.graphx.pkgraph.graph.impl

import org.apache.spark.graphx.pkgraph.compression.K2TreeIndex

case class PKEdge[E](
    var index: K2TreeIndex = K2TreeIndex.initial,
    var line: Long = 0,
    var col: Long = 0,
    var attr: E = null.asInstanceOf[E]
)
