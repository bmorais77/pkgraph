package pt.tecnico.ulisboa.meic.util.collection

import org.apache.spark.util.collection.BitSet
import org.scalatest.FlatSpec

import scala.collection.mutable

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
  "A K²-Tree" should "build from a non-sparse edge list" in {
    val edges = Array((0, 2), (1, 0), (1,1), (2,1), (3,0), (3,3))
    val tree = K2Tree(2, 4, edges)

    assert(tree.k == 2)
    assert(tree.matrixSize == 4)
    assertBitSet(tree.internal, Integer.parseInt("1111", 2))
    assertBitSet(tree.leaves, Integer.parseInt("1000011000011100", 2))
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
   * L: 0011 0000 0110 0000
   */
  it should "build from a sparse edge list" in {
    val edges = Array((1, 0), (1,1), (2,1), (3,0))
    val tree = K2Tree(2, 4, edges)

    assert(tree.k == 2)
    assert(tree.matrixSize == 4)
    assertBitSet(tree.internal, Integer.parseInt("0101", 2))
    assertBitSet(tree.leaves, Integer.parseInt("0000011000001100", 2))
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

    var treeEdges = tree.edges.toArray
    assert(edges sameElements treeEdges)
  }

  private def assertBitSet(bits: BitSet, value: Int): Unit = {
    for(i <- 0 until 32) {
      val mask = 0x01 << i
      val bit = (value & mask) > 0
      assert(bits.get(i) == bit, s"bit $i on bitset did not match bit in value")
    }
  }
}
