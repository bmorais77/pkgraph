package org.apache.spark.graphx.pkgraph.compression

import org.apache.spark.graphx.pkgraph.util.collection.Bitset

case class K2TreeCursor(var bits: Bitset = new Bitset(64), var parentIndex: Int = -1, var sentinel: Int = 0)
