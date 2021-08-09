package org.apache.spark.graphx.pkgraph.graph

import org.scalatest.FlatSpec

class PKGridPartitionStrategySpec extends FlatSpec {
  /**
   * Matrix 4x4:
   * +---+---+---+---+
   * | 0   0   1   0 |
   * | 1   1   0   0 |
   * | 0   1   0   0 |
   * | 1   0   0   1 |
   * +---+---+---+---+
   */
  "A PKGridPartitionStrategy" should "correctly partition a 4x4 matrix (perfect square)" in {
    val edges = Array((1, 0), (1, 1), (0, 2), (2, 1), (3, 0), (3, 3))
    val strategy = new PKGridPartitionStrategy(4)
    val partitions = new Array[Int](4)

    for((line, col) <- edges) {
      val pid = strategy.getPartition(line, col, partitions.length)
      partitions(pid) += 1
    }

    assert(partitions(0) == 2)
    assert(partitions(1) == 1)
    assert(partitions(2) == 2)
    assert(partitions(3) == 1)
  }

  /**
   * Matrix 4x4:
   * +---+---+---+---+
   * | 0   0   1   0 |
   * | 1   1   0   0 |
   * | 0   1   0   0 |
   * | 1   0   0   1 |
   * +---+---+---+---+
   */
  it should "correctly partition a 4x4 matrix (non-perfect square)" in {
    val edges = Array((1, 0), (1, 1), (0, 2), (2, 1), (3, 0), (3, 3))
    val strategy = new PKGridPartitionStrategy(4)
    val partitions = new Array[Int](2)

    for((line, col) <- edges) {
      val pid = strategy.getPartition(line, col, partitions.length)
      partitions(pid) += 1
    }

    assert(partitions(0) == 4)
    assert(partitions(1) == 2)
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
   */
  it should "correctly partition a 8x8 matrix (perfect square)" in {
    val edges = Array((0, 0), (1, 0), (6, 6), (7, 7))
    val strategy = new PKGridPartitionStrategy(8)
    val partitions = new Array[Int](16)

    for((line, col) <- edges) {
      val pid = strategy.getPartition(line, col, partitions.length)
      partitions(pid) += 1
    }

    assert(partitions(0) == 2)
    assert(partitions(15) == 2)
    assert(partitions.forall(i => if(i != 0 && i != 15) partitions(i) == 0 else true))
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
   */
  it should "correctly partition a 8x8 matrix (non-perfect square)" in {
    val edges = Array((0, 0), (1, 0), (6, 6), (7, 7))
    val strategy = new PKGridPartitionStrategy(8)
    val partitions = new Array[Int](10)

    for((line, col) <- edges) {
      val pid = strategy.getPartition(line, col, partitions.length)
      partitions(pid) += 1
    }

    assert(partitions(0) == 2)
    assert(partitions(5) == 2)
    assert(partitions.forall(i => if(i != 0 && i != 5) partitions(i) == 0 else true))
  }
}
