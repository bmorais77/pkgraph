package org.apache.spark.graphx.pkgraph.compression

case class K2TreeEdge private[compression](index: K2TreeIndex, line: Int, col: Int)
