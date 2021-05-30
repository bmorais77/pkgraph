package org.apache.spark.graphx.pkgraph.graph

case class PKEdge[E](
    var index: Int = -1,
    var line: Int = 0,
    var col: Int = 0,
    var attr: E = null.asInstanceOf[E]
)
