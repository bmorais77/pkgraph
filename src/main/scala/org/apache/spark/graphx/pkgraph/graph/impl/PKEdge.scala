package org.apache.spark.graphx.pkgraph.graph.impl

case class PKEdge[E](
    var index: Int = -1,
    var line: Long = 0,
    var col: Long = 0,
    var attr: E = null.asInstanceOf[E]
)
