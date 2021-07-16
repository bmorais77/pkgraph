package org.apache.spark.graphx.pkgraph.compression

import org.scalatest.FlatSpec

import scala.collection.mutable.ArrayBuffer

class K2TreeReverseNeighborIteratorSpec extends FlatSpec {
  /**
   * Matrix 4x4:
   * +---+---+---+---+
   * | 1   0   1   0 |
   * | 1   1   0   0 |
   * | 0   1   0   0 |
   * | 1   0   0   1 |
   * +---+---+---+---+
   *
   * T: 1111
   * L: 1011 1000 0110 0001
   */
  "A K2TreeReverseNeighborIteratorSpec" should "iterate the reverse neighbors in a 4x4 matrix k=2" in {
    val col = 0
    val edges = Array((0, 0), (1, 0), (1,1), (0, 2), (2,1), (3,0), (3,3))
    val tree = K2Tree(2, 4, edges)

    val buffer = new ArrayBuffer[(Int, Int)](edges.length)
    val iterator = new K2TreeReverseNeighborIterator(tree, col)
    while(iterator.hasNext) {
      val edge = iterator.next()
      buffer.append(edge)
    }

    val linesFound = buffer.map(_._1).toArray
    val expectedLines = edges.filter(i => i._2 == col).map(_._1)
    assert(expectedLines sameElements linesFound)
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
   * L: 0010110001001001
   */
  it should "iterate the reverse neighbors in a 4x4 matrix k=4" in {
    val col = 1
    val edges = Array((0, 2), (1, 0), (1,1), (2,1), (3,0), (3,3))
    val tree = K2Tree(4, 4, edges)

    val buffer = new ArrayBuffer[(Int, Int)](edges.length)
    val iterator = new K2TreeReverseNeighborIterator(tree, col)
    while(iterator.hasNext) {
      val edge = iterator.next()
      buffer.append(edge)
    }

    val linesFound = buffer.map(_._1).toArray
    val expectedLines = edges.filter(i => i._2 == col).map(_._1)
    assert(expectedLines sameElements linesFound)
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
  it should "iterate the reverse neighbors in a 8x8 matrix k=2 (1)" in {
    val col = 6
    val edges = Array((0, 0), (1, 0), (6, 6), (7, 7))
    val tree = K2Tree(2, 8, edges)

    val buffer = new ArrayBuffer[(Int, Int)](edges.length)
    val iterator = new K2TreeReverseNeighborIterator(tree, col)
    while(iterator.hasNext) {
      val edge = iterator.next()
      buffer.append(edge)
    }

    val linesFound = buffer.map(_._1).toArray
    val expectedLines = edges.filter(i => i._2 == col).map(_._1)
    assert(expectedLines sameElements linesFound)
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
  it should "iterate the reverse neighbors in a 8x8 matrix k=2 (2)" in {
    val col = 1
    val edges = Array((1, 0), (1,1), (1, 2), (2,1), (3,0), (2, 5), (6, 2), (6, 6))
    val tree = K2Tree(2, 8, edges)

    val buffer = new ArrayBuffer[(Int, Int)](edges.length)
    val iterator = new K2TreeReverseNeighborIterator(tree, col)
    while(iterator.hasNext) {
      val edge = iterator.next()
      buffer.append(edge)
    }

    val linesFound = buffer.map(_._1).toArray
    val expectedLines = edges.filter(i => i._2 == col).map(_._1)
    assert(expectedLines sameElements linesFound)
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
  it should "iterate the reverse neighbors in a 16x16 matrix k=2 (1)" in {
    val col = 11
    val edges = Array((0, 0), (1, 12), (13, 2), (8, 11))
    val tree = K2Tree(2, 16, edges)

    val buffer = new ArrayBuffer[(Int, Int)](edges.length)
    val iterator = new K2TreeReverseNeighborIterator(tree, col)
    while(iterator.hasNext) {
      val edge = iterator.next()
      buffer.append(edge)
    }

    val linesFound = buffer.map(_._1).toArray
    val expectedLines = edges.filter(i => i._2 == col).map(_._1)
    assert(expectedLines sameElements linesFound)
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
   * T: 1001000000101000
   * L: 1000000000000000 0000100000000000 0001000000000000 0000001000000000
   */
  it should "iterate the reverse neighbors a 16x16 matrix k=4 (1)" in {
    val col = 13
    val edges = Array((0, 0), (1, 12), (8, 11), (13, 2))
    val tree = K2Tree(4, 16, edges)

    val buffer = new ArrayBuffer[(Int, Int)](edges.length)
    val iterator = new K2TreeReverseNeighborIterator(tree, col)
    while(iterator.hasNext) {
      val edge = iterator.next()
      buffer.append(edge)
    }

    val linesFound = buffer.map(_._1).toArray
    val expectedLines = edges.filter(i => i._2 == col).map(_._1)
    assert(expectedLines sameElements linesFound)
  }

  it should "iterate the reverse neighbors of a empty tree" in {
    val tree = K2Tree(2, 4, Array.empty)
    val iterator = new K2TreeDirectNeighborIterator(tree, 2)
    assert(!iterator.hasNext)
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
  it should "iterate the reverse neighbors of a dense matrix" in {
    val col = 2
    val edges = (0 until 4)
      .flatMap(i => (0 until 4).map(j => (i, j, K2TreeIndex.fromEdge(2, 2, i, j))))
      .sortBy(_._3)
      .map(a => (a._1, a._2))
      .toArray

    val tree = K2Tree(2, 4, edges)
    val buffer = new ArrayBuffer[(Int, Int)](edges.length)

    val iterator = new K2TreeReverseNeighborIterator(tree, col)
    while(iterator.hasNext) {
      val edge = iterator.next()
      buffer.append(edge)
    }

    val edgesFound: Array[(Int, Int)] = buffer.toArray
    val expectedEdges =
      (0 until 4).map(i => (i, col)).map(i => (i._1, K2TreeIndex.fromEdge(2, tree.height, i._1, i._2))).toArray
    assert(expectedEdges sameElements edgesFound)
  }
}
