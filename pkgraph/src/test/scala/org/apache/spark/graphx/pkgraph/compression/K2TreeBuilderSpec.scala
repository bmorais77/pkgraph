package org.apache.spark.graphx.pkgraph.compression

import org.apache.spark.graphx.pkgraph.TestUtils
import org.scalatest.FlatSpec

class K2TreeBuilderSpec extends FlatSpec {
  /**
   * Matrix 16x16:
   * +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
   * | 1   0   0   0 | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 |
   * | 0   1   0   0 | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 |
   * | 0   0   1   0 | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 |
   * | 0   0   0   1 | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 |
   * |---+---+---+---|---+---+---+---|---+---+---+---|---+---+---+---|
   * | 0   0   0   0 | 1   0   0   0 | 0   0   0   0 | 0   0   0   0 |
   * | 0   0   0   0 | 0   1   0   0 | 0   0   0   0 | 0   0   0   0 |
   * | 0   0   0   0 | 0   0   1   0 | 0   0   0   0 | 0   0   0   0 |
   * | 0   0   0   0 | 0   0   0   1 | 0   0   0   0 | 0   0   0   0 |
   * +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
   * | 0   0   0   0 | 0   0   0   0 | 1   0   0   0 | 0   0   0   0 |
   * | 0   0   0   0 | 0   0   0   0 | 0   1   0   0 | 0   0   0   0 |
   * | 0   0   0   0 | 0   0   0   0 | 0   0   1   0 | 0   0   0   0 |
   * | 0   0   0   0 | 0   0   0   0 | 0   0   0   1 | 0   0   0   0 |
   * |---+---+---+---|---+---+---+---|---+---+---+---|---+---+---+---|
   * | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 | 1   0   0   0 |
   * | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 | 0   1   0   0 |
   * | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 | 0   0   1   0 |
   * | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 | 0   0   0   1 |
   * +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
   *
   * T: 1001 1001 1001 1001 1001 1001 1001
   * L: 1001 1001 1001 1001 1001 1001 1001 1001
   */
  "A K²-Tree Builder" should "build from and to compressed tree (16x16)" in {
    val tree = K2Tree(2, 16, (0 until 16).map(i => (i, i)).toArray)
    TestUtils.assertBitSet(tree.bits, "1001 1001 1001 1001 1001 1001 1001 1001 1001 1001 1001 1001 1001 1001 1001")

    val builder = K2TreeBuilder.fromK2Tree(tree)
    val treeFromBuilder = builder.build
    TestUtils.assertBitSet(treeFromBuilder.bits, "1001 1001 1001 1001 1001 1001 1001 1001 1001 1001 1001 1001 1001 1001 1001")
  }

  /**
   * Matrix 8x8:
   * +---+---+---+---+---+---+---+---+
   * | 1   0   0   0 | 0   0   0   0 |
   * | 0   1   0   0 | 0   0   0   0 |
   * | 0   0   1   0 | 0   0   0   0 |
   * | 0   0   0   1 | 0   0   0   0 |
   * |---+---+---+---|---+---+---+---|
   * | 0   0   0   0 | 1   0   0   0 |
   * | 0   0   0   0 | 0   1   0   0 |
   * | 0   0   0   0 | 0   0   1   0 |
   * | 0   0   0   0 | 0   0   0   1 |
   * +---+---+---+---+---+---+---+---+
   *
   * T: 1001 1001 1001
   * L: 1001 1001 1001 1001
   */
  it should "build an uncompressed tree (8x8)" in {
    val tree = K2Tree(2, 8, (0 until 16).map(i => (i, i)).toArray)
    TestUtils.assertBitSet(tree.bits, "1001 1001 1001 1001 1001 1001 1001")

    val builder = K2TreeBuilder.fromK2Tree(tree)
    TestUtils.assertBitSet(builder.bits, "1001 1001 0000 0000 1001 1001 0000 0000 1001 0000 0000 0000 0000 0000 0000 0000 0000 1001 0000 0000 1001")
  }

  it should "build with very large sizes" in {
    val tree = K2TreeBuilder(2, 100000)
    assert(tree.size.toDouble * tree.size.toDouble >= 100000)
  }

  it should "round the size of the tree to the nearest power of k" in {
    val b1 = K2TreeBuilder(2, 10)
    assert(b1.size == 16)

    val b2 = K2TreeBuilder(2, 8)
    assert(b2.size == 8)
  }

  /**
   * Indices of a matrix 8x8 with k=2:
   * +---+---+---+---+---+---+---+---+
   * | 0   1   4   5 | 16 17   20 21 |
   * | 2   3   6   7 | 18 19   22 23 |
   * | 8   9   12 13 | 24 25   28 29 |
   * | 10 11   14 15 | 26 27   30 31 |
   * |---+---+---+---|---+---+---+---|
   * | 32 33   36 37 | 48 49   52 53 |
   * | 34 35   38 39 | 50 51   54 55 |
   * | 40 41   44 45 | 56 57   60 61 |
   * | 42 43   46 47 | 58 59   62 63 |
   * +---+---+---+---+---+---+---+---+
   */
  it should "add/remove edge and return correct index" in {
    val builder = K2TreeBuilder(2, 8)
    val insertIndex = builder.addEdge(4, 1)
    assert(insertIndex == 33)

    val removeIndex = builder.removeEdge(4, 1)
    assert(removeIndex == insertIndex)
  }
}
