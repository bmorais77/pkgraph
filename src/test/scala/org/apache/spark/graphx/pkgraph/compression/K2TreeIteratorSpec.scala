package org.apache.spark.graphx.pkgraph.compression

import org.scalatest.FlatSpec

import scala.collection.mutable.ArrayBuffer

class K2TreeIteratorSpec extends FlatSpec {
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
  "A K²-Tree Iterator" should "iterate a 4x4 matrix k=2 (1)" in {
    val edges = Array((1, 0), (1,1), (0, 2), (2,1), (3,0), (3,3))
    val tree = K2Tree(2, 4, edges)

    val buffer = new ArrayBuffer[(Int, Int)](tree.edgeCount)
    val iterator = new K2TreeIterator(tree)
    while(iterator.hasNext) {
      val edge = iterator.next()
      buffer.append((edge.line, edge.col))
    }

    val edgesFound: Array[(Int, Int)] = buffer.toArray
    assert(edges sameElements edgesFound)
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
  it should "iterate a 4x4 matrix k=2 in reverse (1)" in {
    val edges = Array((1, 0), (1,1), (0, 2), (2,1), (3,0), (3,3)).reverse
    val tree = K2Tree(2, 4, edges)

    val buffer = new ArrayBuffer[(Int, Int)](tree.edgeCount)
    val iterator = new K2TreeIterator(tree,  true)
    while(iterator.hasNext) {
      val edge = iterator.next()
      buffer.append((edge.line, edge.col))
    }

    val edgesFound: Array[(Int, Int)] = buffer.toArray
    assert(edges sameElements edgesFound)
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
  it should "iterate a 4x4 matrix k=4 (1)" in {
    val edges = Array((0, 2), (1, 0), (1,1), (2,1), (3,0), (3,3))
    val tree = K2Tree(4, 4, edges)

    val buffer = new ArrayBuffer[(Int, Int)](tree.edgeCount)
    val iterator = new K2TreeIterator(tree)
    while(iterator.hasNext) {
      val edge = iterator.next()
      buffer.append((edge.line, edge.col))
    }

    val edgesFound: Array[(Int, Int)] = buffer.toArray
    assert(edges sameElements edgesFound)
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
  it should "iterate a 4x4 matrix k=4 in reverse (1)" in {
    val edges = Array((0, 2), (1, 0), (1,1), (2,1), (3,0), (3,3)).reverse
    val tree = K2Tree(4, 4, edges)

    val buffer = new ArrayBuffer[(Int, Int)](tree.edgeCount)
    val iterator = new K2TreeIterator(tree, true)
    while(iterator.hasNext) {
      val edge = iterator.next()
      buffer.append((edge.line, edge.col))
    }

    val edgesFound: Array[(Int, Int)] = buffer.toArray
    assert(edges sameElements edgesFound)
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
  it should "iterate a 8x8 matrix k=2 (1)" in {
    val edges = Array((0, 0), (1, 0), (6, 6), (7, 7))
    val tree = K2Tree(2, 8, edges)

    val buffer = new ArrayBuffer[(Int, Int)](tree.edgeCount)
    val iterator = new K2TreeIterator(tree)
    while(iterator.hasNext) {
      val edge = iterator.next()
      buffer.append((edge.line, edge.col))
    }

    val edgesFound: Array[(Int, Int)] = buffer.toArray
    assert(edges sameElements edgesFound)
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
  it should "iterate a 8x8 matrix k=2 in reverse (1)" in {
    val edges = Array((0, 0), (1, 0), (6, 6), (7, 7)).reverse
    val tree = K2Tree(2, 8, edges)

    val buffer = new ArrayBuffer[(Int, Int)](tree.edgeCount)
    val iterator = new K2TreeIterator(tree, true)
    while(iterator.hasNext) {
      val edge = iterator.next()
      buffer.append((edge.line, edge.col))
    }

    val edgesFound: Array[(Int, Int)] = buffer.toArray
    assert(edges sameElements edgesFound)
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
  it should "iterate a 8x8 matrix k=2 (2)" in {
    val edges = Array((1, 0), (1,1), (1, 2), (2,1), (3,0), (2, 5), (6, 2), (6, 6))
    val tree = K2Tree(2, 8, edges)

    val buffer = new ArrayBuffer[(Int, Int)](tree.edgeCount)
    val iterator = new K2TreeIterator(tree)
    while(iterator.hasNext) {
      val edge = iterator.next()
      buffer.append((edge.line, edge.col))
    }

    val edgesFound: Array[(Int, Int)] = buffer.toArray
    assert(edges sameElements edgesFound)
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
  it should "iterate a 8x8 matrix k=2 in reverse (2)" in {
    val edges = Array((1, 0), (1,1), (1, 2), (2,1), (3,0), (2, 5), (6, 2), (6, 6)).reverse
    val tree = K2Tree(2, 8, edges)

    val buffer = new ArrayBuffer[(Int, Int)](tree.edgeCount)
    val iterator = new K2TreeIterator(tree, true)
    while(iterator.hasNext) {
      val edge = iterator.next()
      buffer.append((edge.line, edge.col))
    }

    val edgesFound: Array[(Int, Int)] = buffer.toArray
    assert(edges sameElements edgesFound)
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
  it should "iterate a 16x16 matrix k=2 (1)" in {
    val edges = Array((0, 0), (1, 12), (13, 2), (8, 11))
    val tree = K2Tree(2, 16, edges)

    val buffer = new ArrayBuffer[(Int, Int)](tree.edgeCount)
    val iterator = new K2TreeIterator(tree)
    while(iterator.hasNext) {
      val edge = iterator.next()
      buffer.append((edge.line, edge.col))
    }

    val edgesFound: Array[(Int, Int)] = buffer.toArray
    assert(edges sameElements edgesFound)
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
  it should "iterate a 16x16 matrix k=2 in reverse (1)" in {
    val edges = Array((0, 0), (1, 12), (13, 2), (8, 11)).reverse
    val tree = K2Tree(2, 16, edges)

    val buffer = new ArrayBuffer[(Int, Int)](tree.edgeCount)
    val iterator = new K2TreeIterator(tree, true)
    while(iterator.hasNext) {
      val edge = iterator.next()
      buffer.append((edge.line, edge.col))
    }

    val edgesFound: Array[(Int, Int)] = buffer.toArray
    assert(edges sameElements edgesFound)
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
  it should "iterate a 16x16 matrix k=4 (1)" in {
    val edges = Array((0, 0), (1, 12), (8, 11), (13, 2))
    val tree = K2Tree(4, 16, edges)

    val buffer = new ArrayBuffer[(Int, Int)](tree.edgeCount)
    val iterator = new K2TreeIterator(tree)
    while(iterator.hasNext) {
      val edge = iterator.next()
      buffer.append((edge.line, edge.col))
    }

    val edgesFound: Array[(Int, Int)] = buffer.toArray
    assert(edges sameElements edgesFound)
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
  it should "iterate a 16x16 matrix k=4 in reverse (1)" in {
    val edges = Array((0, 0), (1, 12), (8, 11), (13, 2)).reverse
    val tree = K2Tree(4, 16, edges)

    val buffer = new ArrayBuffer[(Int, Int)](tree.edgeCount)
    val iterator = new K2TreeIterator(tree, true)
    while(iterator.hasNext) {
      val edge = iterator.next()
      buffer.append((edge.line, edge.col))
    }

    val edgesFound: Array[(Int, Int)] = buffer.toArray
    assert(edges sameElements edgesFound)
  }

  it should "iterate a empty tree" in {
    val tree = K2Tree(2, 4, Array.empty)
    val iterator = new K2TreeIterator(tree)
    assert(!iterator.hasNext)
  }
}
