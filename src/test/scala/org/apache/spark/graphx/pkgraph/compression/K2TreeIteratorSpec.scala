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
  "A K²-Tree Iterator" should "iterate a 4x4 tree (k=2)" in {
    val edges = Array((1, 0), (1,1), (0, 2), (2,1), (3,0), (3,3))
    val tree = K2Tree(2, 4, edges)

    val buffer = new ArrayBuffer[(Int, Int)](tree.edgeCount)
    val iterator = new K2TreeIterator(tree)
    while(iterator.hasNext) {
      val edge = iterator.next()
      buffer.append(edge)
    }

    val edgesFound: Array[(Int, Int)] = buffer.toArray
    assert(edges sameElements edgesFound)
  }
}
