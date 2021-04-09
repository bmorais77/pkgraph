package org.apache.spark.graphx.pkgraph.compression

// TODO: Make compare work with different K values
case class K2TreeIndex private[compression](k: Int, size: Int, indices: Array[Int]) extends Ordered[K2TreeIndex] {
  override def compare(other: K2TreeIndex): Int = {
    var thisSize = size
    var otherSize = other.size
    var thisIndex = 0
    var otherIndex = 0
    var i = 0
    var j = 0

    while (i < indices.length || j < other.indices.length) {
      if(i < indices.length) {
        thisIndex += indices(i) * thisSize
        thisSize /= k
        i += 1
      }

      if(j < other.indices.length) {
        otherIndex += other.indices(j) * otherSize
        otherSize /= other.k
        j += 1
      }
    }

    thisIndex - otherIndex
  }
}

object K2TreeIndex {
  val initial: K2TreeIndex = K2TreeIndex(1, 1, Array(-1))

  def fromEdge(k: Int, height: Int, line: Int, col: Int): K2TreeIndex = {
    val size = math.pow(k, height - 1).toInt
    val indices = new Array[Int](height)

    var currLine = line
    var currCol = col
    var i = height - 1

    while(i >= 0) {
      indices(i) = (currLine % k) * k + (currCol % k)

      currLine /= k
      currCol /= k
      i -= 1
    }
    K2TreeIndex(k, size, indices)
  }
}