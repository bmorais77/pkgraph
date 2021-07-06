package org.apache.spark.graphx.pkgraph.compression

import org.scalatest.FlatSpec

class K2TreeIndexSpec extends FlatSpec {
  "A K2TreeIndex" should "build from edge" in {
    val index = K2TreeIndex.fromEdge(2, 3, 3, 3)
    assert(index == 15)
  }

  it should "compare with other equal indices with same size" in {
    val i1 = K2TreeIndex.fromEdge(2, 3, 4, 4)
    val i2 = K2TreeIndex.fromEdge(2, 3, 4, 4)
    assert(i1 == i2)
  }

  it should "compare with other equal indices with different size" in {
    val i1 = K2TreeIndex.fromEdge(2, 3, 4, 4)
    val i2 = K2TreeIndex.fromEdge(2, 4, 4, 4)
    assert(i1 == i2)
  }

  it should "compare with other indices of same size" in {
    val i1 = K2TreeIndex.fromEdge(2, 3, 3, 3)
    val i2 = K2TreeIndex.fromEdge(2, 3, 4, 4)
    assert(i1 < i2)
  }

  it should "compare with other indices of different size" in {
    val i1 = K2TreeIndex.fromEdge(2, 3, 4, 3)
    val i2 = K2TreeIndex.fromEdge(2, 4, 4, 4)
    assert(i1 < i2)
  }

  /**
   * Matrix 4x4:
   * +---+---+---+---+
   * | 1   1   1   1 |
   * | 1   1   1   1 |
   * | 1   1   1   1 |
   * | 1   1   1   1 |
   * +---+---+---+---+
   */
  it should "build correct indices for every edge in the matrix" in {
    val edges = (0 until 4)
      .flatMap(i => (0 until 4).map(j => (i, j, K2TreeIndex.fromEdge(2, 2, i, j))))
      .sortBy(_._3)
      .toArray

    var i = 0
    for((_, _, index) <- edges) {
      assert(index == i)
      i += 1
    }
    assert(i == 16)
  }

  it should "build correct indices for large line/column" in {
    val idx1 = K2TreeIndex.fromEdge(8, 10, 298385420, 294846486)
    val idx2 = K2TreeIndex.fromEdge(8, 10, 20, 2425151)
    assert(idx2 < idx1)
  }
}
