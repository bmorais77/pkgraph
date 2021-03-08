package pt.tecnico.ulisboa.meic.util.collection

import org.apache.spark.util.collection.BitSet
import org.scalatest.FlatSpec
import pt.tecnico.ulisboa.meic.compression.K2Tree

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
    assert(tree.size == 4)
    assertBitSet(tree.bits, "1111 0011 1000 0110 0001")
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
  it should "build from 8x8 matrix (1)" in {
    val edges = Array((0, 0), (1, 0), (6, 6), (7, 7))
    val tree = K2Tree(2, 8, edges)

    assert(tree.k == 2)
    assert(tree.size == 8)
    assertBitSet(tree.bits, "1001 1000 0001 1010 1001")
  }

  /**
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
  it should "build from 8x8 matrix (2)" in {
    val edges = Array((1, 0), (1,1), (2,1), (3,0), (1, 2), (2, 5), (6, 2), (6, 6))
    val tree = K2Tree(2, 8, edges)

    assert(tree.k == 2)
    assert(tree.size == 8)
    assertBitSet(tree.bits, "1111 1110 0010 0001 0001 0011 0010 0110 0100 1000 1000")
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
    assert(tree.size == 16)
    assertBitSet(tree.bits, "1111 1000 0100 0010 1000 1000 1000 0100 0100 1000 0010 0010 0100")
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
    assert(tree.size == 4)
    assertBitSet(tree.bits, "1010 0011 0110")
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
    assert(tree.size == 4)
    assertBitSet(tree.bits, "1111 1111 1111 1111 1111")
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
    assert(tree.size == 4)
    assertBitSet(tree.bits, "1010 0011 0110")

    val newEdges = Array((0, 2), (0, 3), (2, 3), (3, 2))
    val newTree = tree.append(4, newEdges)

    assert(newTree.k == 2)
    assert(newTree.size == 4)
    assertBitSet(newTree.bits, "1111 0011 1100 0110 0110")
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
    assert(tree.size == 4)
    assertBitSet(tree.bits, "1010 0011 0110")

    val newEdges = Array((1, 2), (2, 5), (6, 2), (6, 6))
    val newTree = tree.append(8, newEdges)

    assert(newTree.k == 2)
    assert(newTree.size == 8)
    assertBitSet(newTree.bits, "1111 1110 0010 0001 0001 0011 0010 0110 0100 1000 1000")
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

    val treeEdges = tree.edges
    assert(edges sameElements treeEdges)
  }

  private def assertBitSet(bits: BitSet, binary: String): Unit = {
    val compactBinary = binary.replaceAll("\\s", "")
    for(i <- 0 until compactBinary.length) {
      val bit = compactBinary.charAt(i) == '1'
      assert(bits.get(i) == bit, s"bit $i on bitset did not match bit in value")
    }
  }
}
