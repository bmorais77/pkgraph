package org.apache.spark.graphx.pkgraph.compression

import org.scalatest.FlatSpec

class K2TreeReverseNeighborIteratorSpec extends FlatSpec {
  /**
   * Matrix 8x8:
   * +---+---+---+---+---+---+---+---+
   * | 0   1   0   0   0   0   0   0 |
   * | 0   0   0   0   0   0   0   0 |
   * | 0   1   0   0   0   0   0   0 |
   * | 0   1   0   0   0   0   0   0 |
   * | 0   1   0   0   0   0   0   0 |
   * | 0   0   0   0   0   0   0   0 |
   * | 0   0   0   0   0   0   0   0 |
   * | 0   1   0   0   0   0   0   0 |
   * +---+---+---+---+---+---+---+---+
   */
  "A K²-Tree reverse neighbor iterator" should "find all reverse neighbors of vertex" in {
    val edges = Array((0, 1), (2, 1), (3, 1), (4, 1), (7, 1))
    val tree = K2Tree(2, 8, edges)

    val neighbors = tree.reverseNeighbors(1)
    val expected = Array(0, 2, 3, 4, 7)
    assert(expected sameElements neighbors)
  }
}
