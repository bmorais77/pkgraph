package org.apache.spark.graphx.pkgraph.util

import org.apache.spark.util.collection.BitSet

object TestUtils {
  def assertBitSet(bits: BitSet, binary: String): Unit = {
    val compactBinary = binary.replaceAll("\\s", "")
    for (i <- 0 until compactBinary.length) {
      val bit = compactBinary.charAt(i) == '1'
      assert(bits.get(i) == bit, s"bit $i on bitset did not match bit in value")
    }
  }
}
