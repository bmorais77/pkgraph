package org.apache.spark.graphx.pkgraph.graph.impl

import org.apache.spark.graphx.VertexId

case class TreeEdge[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED](
    var srcId: VertexId = 0,
    var dstId: VertexId = 0,
    var attr: ED = null.asInstanceOf[ED],
    var pos: Int = 0
) extends Serializable
