package org.apache.spark.graphx.pkgraph.compression

import org.apache.spark.graphx.pkgraph.TestUtils
import org.scalatest.FlatSpec

class K2TreeBuilderSpec extends FlatSpec {

  /**
    * Matrix 4x4:
    * +---+---+---+---+
    * | 0   1   0   0 |
    * | 1   0   0   0 |
    * | 0   1   0   1 |
    * | 0   1   0   0 |
    * |---+---+---+---|
    *
    * T: 1011
    * L: 0110 0101 0100
    */
  "A K²-Tree Builder" should "build from a 4x4 matrix" in {
    val builder = K2TreeBuilder(2, 4)
    val edges = Seq((0, 1), (1, 0), (2, 1), (3, 1), (2, 3))

    for ((line, col) <- edges) {
      builder.addEdge(line, col)
    }

    val tree = builder.build()
    TestUtils.assertBitSet(tree.bits, "1011 0110 0101 0100")
  }
}
