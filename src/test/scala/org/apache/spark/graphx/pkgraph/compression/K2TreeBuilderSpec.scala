package org.apache.spark.graphx.pkgraph.compression

import org.apache.spark.graphx.pkgraph.util.TestUtils
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

  it should "round the size of the tree to the nearest power of k" in {
    val b1 = K2TreeBuilder(2, 10)
    assert(b1.size == 16)

    val b2 = K2TreeBuilder(2, 8)
    assert(b2.size == 8)
  }
}