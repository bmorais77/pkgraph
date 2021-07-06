package org.apache.spark.graphx.pkgraph.compression

object K2TreeIndex {
  def fromEdge(k: Int, height: Int, line: Int, col: Int): Long = {
    val k2 = k * k
    var currLine = line
    var currCol = col
    var currSize = 1L
    var h = height
    var index = 0L

    while (h > 0) {
      val localIndex = (currLine % k) * k + (currCol % k)
      index += localIndex * currSize

      currLine /= k
      currCol /= k
      currSize *= k2
      h -= 1
    }

    index
  }
}
