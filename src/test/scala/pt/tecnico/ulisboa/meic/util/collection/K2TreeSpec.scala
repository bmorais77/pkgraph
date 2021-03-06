package pt.tecnico.ulisboa.meic.util.collection

import org.apache.spark.util.collection.BitSet
import org.scalatest.FlatSpec
import pt.tecnico.ulisboa.meic.compression.K2Tree

import java.math.BigInteger

class K2TreeSpec extends FlatSpec {
  /**
   * Matrix 4x4:
   * +---+---+---+---+
   * | 0   0   1   0 |
   * | 1   1   0   0 |
   * | 0   1   0   0 |
   * | 1   0   0   1 |
   * +---+---+---+---+
   *
   * T: 1111
   * L: 0011 1000 0110 0001
   */
  "A K²-Tree" should "build from 4x4 matrix" in {
    val edges = Array((1, 0), (1,1), (0, 2), (2,1), (3,0), (3,3))
    val tree = K2Tree(2, 4, edges)

    assert(tree.k == 2)
    assert(tree.matrixSize == 4)
    assertBitSet(tree.internal, Integer.parseInt("1111", 2))
    assertBitSet(tree.leaves, Integer.parseInt("1000011000011100", 2))
  }

  /**
   * Matrix 8x8:
   * +---+---+---+---+---+---+---+---+
   * | 1   0   0   0   0   0   0   0 |
   * | 1   0   0   0   0   0   0   0 |
   * | 0   0   0   0   0   0   0   0 |
   * | 0   0   0   0   0   0   0   0 |
   * | 0   0   0   0   0   0   0   0 |
   * | 0   0   0   0   0   0   0   0 |
   * | 0   0   0   0   0   0   1   0 |
   * | 0   0   0   0   0   0   0   1 |
   * +---+---+---+---+---+---+---+---+
   *
   * T: 1001 1000 0001
   * L: 1010 1001
   */
  it should "build from 8x8 matrix" in {
    val edges = Array((0, 0), (1, 0), (6, 6), (7, 7))
    val tree = K2Tree(2, 8, edges)

    assert(tree.k == 2)
    assert(tree.matrixSize == 8)
    assertBitSet(tree.internal, Integer.parseInt("100000011001", 2))
    assertBitSet(tree.leaves, Integer.parseInt("10010101", 2))
  }

  /**
   * Matrix 16x16:
   * +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
   * | 1   0   0   0 | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 |
   * | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 | 1   0   0   0 |
   * | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 |
   * | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 |
   * |---+---+---+---|---+---+---+---|---+---+---+---|---+---+---+---|
   * | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 |
   * | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 |
   * | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 |
   * | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 |
   * +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
   * | 0   0   0   0 | 0   0   0   0 | 0   0   0   1 | 0   0   0   0 |
   * | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 |
   * | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 |
   * | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 |
   * |---+---+---+---|---+---+---+---|---+---+---+---|---+---+---+---|
   * | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 |
   * | 0   0   1   0 | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 |
   * | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 |
   * | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 |
   * +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
   *
   * T: 1111 1000 0100 0010 1000 1000 1000 0100 0100
   * L: 1000 0010 0010 0100
   */
  it should "build from 16x16 matrix" in {
    val edges = Array((0, 0), (1, 12), (13, 2), (8, 11))
    val tree = K2Tree(2, 16, edges)

    assert(tree.k == 2)
    assert(tree.matrixSize == 16)
    assertBitSet(tree.internal, new BigInteger("001000100001000100010100001000011111", 2).longValue())
    assertBitSet(tree.leaves, Integer.parseInt("0010010001000001", 2))
  }

  /**
   * Matrix 4x4:
   * +---+---+---+---+
   * | 0   0   0   0 |
   * | 1   1   0   0 |
   * | 0   1   0   0 |
   * | 1   0   0   0 |
   * +---+---+---+---+
   *
   * T: 1010
   * L: 0011 0110
   */
  it should "build from a sparse edge list" in {
    val edges = Array((1, 0), (1,1), (2,1), (3,0))
    val tree = K2Tree(2, 4, edges)

    assert(tree.k == 2)
    assert(tree.matrixSize == 4)
    assertBitSet(tree.internal, Integer.parseInt("0101", 2))
    assertBitSet(tree.leaves, Integer.parseInt("01101100", 2))
  }

  /**
   * Matrix 4x4:
   * +---+---+---+---+
   * | 1   1   1   1 |
   * | 1   1   1   1 |
   * | 1   1   1   1 |
   * | 1   1   1   1 |
   * +---+---+---+---+
   *
   * T: 1111
   * L: 1111 1111 1111 1111
   */
  it should "build from a complete edge list" in {
    val edges = (0 until 4).flatMap(i => (0 until 4).map(j => (i ,j))).toArray
    val tree = K2Tree(2, 4, edges)

    assert(tree.k == 2)
    assert(tree.matrixSize == 4)
    assertBitSet(tree.internal, Integer.parseInt("1111", 2))
    assertBitSet(tree.leaves, Integer.parseInt("1111111111111111", 2))
  }

  /**
   * - Before append:
   *
   * Matrix 4x4:
   * +---+---+---+---+
   * | 0   0   0   0 |
   * | 1   1   0   0 |
   * | 0   1   0   0 |
   * | 1   0   0   0 |
   * +---+---+---+---+
   *
   * T: 1010
   * L: 0011 0110
   *
   * - After append:
   *
   * Matrix 4x4:
   * +---+---+---+---+
   * | 0   0   1   1 |
   * | 1   1   0   0 |
   * | 0   1   0   1 |
   * | 1   0   1   0 |
   * +---+---+---+---+
   *
   * T: 1111
   * L: 0011 1100 0110 0110
   */
  it should "append new edges without growing matrix" in {
    val edges = Array((1, 0), (1,1), (2,1), (3,0))
    val tree = K2Tree(2, 4, edges)

    assert(tree.k == 2)
    assert(tree.matrixSize == 4)
    assertBitSet(tree.internal, Integer.parseInt("0101", 2))
    assertBitSet(tree.leaves, Integer.parseInt("01101100", 2))

    val newEdges = Array((0, 2), (0, 3), (2, 3), (3, 2))
    val newTree = tree.append(4, newEdges)

    assert(newTree.k == 2)
    assert(newTree.matrixSize == 4)
    assertBitSet(newTree.internal, Integer.parseInt("1111", 2))
    assertBitSet(newTree.leaves, Integer.parseInt("0110011000111100", 2))
  }

  /**
   * - Before append:
   *
   * Matrix 4x4:
   * +---+---+---+---+
   * | 0   0   0   0 |
   * | 1   1   0   0 |
   * | 0   1   0   0 |
   * | 1   0   0   0 |
   * +---+---+---+---+
   *
   * T: 1010
   * L: 0011 0110
   *
   * - After append:
   *
   * Matrix 8x8:
   * +---+---+---+---+---+---+---+---+
   * | 0   0   0   0   0   0   0   0 |
   * | 1   1   1   0   0   0   0   0 |
   * | 0   1   0   0   0   1   0   0 |
   * | 1   0   0   0   0   0   0   0 |
   * | 0   0   0   0   0   0   0   0 |
   * | 0   0   0   0   0   0   0   0 |
   * | 0   0   1   0   0   0   1   0 |
   * | 0   0   0   0   0   0   0   0 |
   * +---+---+---+---+---+---+---+---+
   *
   * T: 1111 1110 0010 0001 0001
   * L: 0011 0010 0110 0100 1000 1000
   */
  it should "append new edges with growing matrix" in {
    val edges = Array((1, 0), (1,1), (2,1), (3,0))
    val tree = K2Tree(2, 4, edges)

    assert(tree.k == 2)
    assert(tree.matrixSize == 4)
    assertBitSet(tree.internal, Integer.parseInt("0101", 2))
    assertBitSet(tree.leaves, Integer.parseInt("01101100", 2))

    val newEdges = Array((1, 2), (2, 5), (6, 2), (6, 6))
    val newTree = tree.append(8, newEdges)

    assert(newTree.k == 2)
    assert(newTree.matrixSize == 8)
    assertBitSet(newTree.internal, Integer.parseInt("10001000010001111111", 2))
    assertBitSet(newTree.leaves, Integer.parseInt("000100010010011001001100", 2))
  }

  /**
   * Matrix 4x4:
   * +---+---+---+---+
   * | 0   0   1   0 |
   * | 1   1   0   0 |
   * | 0   1   0   0 |
   * | 1   0   0   1 |
   * +---+---+---+---+
   *
   * T: 1111
   * L: 0011 1000 0110 0001
   */
  it should "iterate all edges in tree" in {
    val edges = Array((1, 0), (1,1), (0, 2), (2,1), (3,0), (3,3))
    val tree = K2Tree(2, 4, edges)

    val treeEdges = tree.toArray
    assert(edges sameElements treeEdges)
  }

  private def assertBitSet(bits: BitSet, value: Long): Unit = {
    for(i <- 0 until 64) {
      val mask = 0x01.toLong << i
      val bit = (value & mask) > 0
      assert(bits.get(i) == bit, s"bit $i on bitset did not match bit in value")
    }
  }
}
