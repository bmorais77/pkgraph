package org.apache.spark.graphx.pkgraph.compression

import org.scalatest.FlatSpec

class K2TreeDirectNeighborIteratorSpec extends FlatSpec {
  /**
   * Matrix 8x8:
   * +---+---+---+---+---+---+---+---+
   * | 0   0   0   0   0   0   0   0 |
   * | 1   0   1   1   1   0   0   1 |
   * | 0   0   0   0   0   0   0   0 |
   * | 0   0   0   0   0   0   0   0 |
   * | 0   0   0   0   0   0   0   0 |
   * | 0   0   0   0   0   0   0   0 |
   * | 0   0   0   0   0   0   0   0 |
   * | 0   0   0   0   0   0   0   0 |
   * +---+---+---+---+---+---+---+---+
   */
  "A K²-Tree direct neighbor iterator" should "find all direct neighbors of a line" in {
    val edges = Array((1, 0), (1, 2), (1, 3), (1, 4), (1, 7))
    val tree = K2Tree(2, 8, edges)
    val neighbors = tree.directNeighbors(1)
    val expected = Array(0, 2, 3, 4, 7)
    assert(expected sameElements neighbors)
  }

  /**
   * Matrix 8x8:
   * +---+---+---+---+---+---+---+---+
   * | 1   0   0   0   0   0   0   0 |
   * | 0   1   0   0   0   0   0   0 |
   * | 0   0   1   0   0   0   0   0 |
   * | 0   0   0   1   0   0   0   0 |
   * | 0   0   0   0   1   0   0   0 |
   * | 0   0   0   0   0   1   0   0 |
   * | 0   0   0   0   0   0   1   0 |
   * | 0   0   0   0   0   0   0   1 |
   * +---+---+---+---+---+---+---+---+
   */
  it should "find all direct neighbors of vertices in the diagonal matrix" in {
    val edges = (0 until 8).map(i => (i, i)).toArray
    val tree = K2Tree(2, 8, edges)

    var i = 0
    while(i < 8) {
      val neighbors = tree.directNeighbors(i)
      val expected = Array(i)
      assert(expected sameElements neighbors)
      i += 1
    }
    assert(i == 8)
  }
}
