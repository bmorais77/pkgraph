package org.apache.spark.graphx.pkgraph.compression

case class K2TreeIndex private[compression](indices: Array[Int]) extends Ordered[K2TreeIndex] {
  override def compare(other: K2TreeIndex): Int = {
    var i = 0
    var j = 0

    while (i < indices.length && j < other.indices.length) {
      if (indices(i) < other.indices(j)) {
        return -1
      } else if (indices(i) > other.indices(j)) {
        return 1
      }

      i += 1
      j += 1
    }

    0
  }
}
