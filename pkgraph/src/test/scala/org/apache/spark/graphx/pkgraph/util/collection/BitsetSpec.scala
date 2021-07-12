package org.apache.spark.graphx.pkgraph.util.collection

import org.scalatest.FlatSpec

class BitsetSpec extends FlatSpec {
  "A PKBitSet" should "count the number of bits between range" in {
    val bits = new Bitset(64 * 3)

    // 1st word
    bits.set(0)
    bits.set(10)
    bits.set(20)
    bits.set(30)
    bits.set(63)

    // 2nd word
    bits.set(64)
    bits.set(70)
    bits.set(80)
    bits.set(89)
    bits.set(90)
    bits.set(100)
    bits.set(127)

    // 3rd word
    bits.set(128)
    bits.set(140)
    bits.set(141)
    bits.set(64 * 3 - 1)

    assert(bits.count(0, -1) == 0)
    assert(bits.count(0, 0) == 1)
    assert(bits.count(0, 10) == 2)
    assert(bits.count(0, 63) == 5)
    assert(bits.count(0, 64) == 6)
    assert(bits.count(0, 89) == 9)
    assert(bits.count(0, 64 * 3 - 1) == 16)
    assert(bits.count(10, 64 * 3 - 1) == 15)
    assert(bits.count(63, 64) == 2)
    assert(bits.count(63, 127) == 8)
    assert(bits.count(63, 128) == 9)
    assert(bits.count(63, 64 * 3 - 1) == 12)
    assert(bits.count(64, 64 * 3 - 1) == 11)
  }

  it should "shift bits left" in {
    val bits = new Bitset(64 * 3)
    val values = Array(0, 1, 2, 10, 11, 12, 63, 64, 66, 80, 110, 128, 129)

    for (value <- values) {
      bits.set(value)
    }

    val start = 10
    val offset = 4
    val shiftedBits = bits.shiftLeft(start, offset)
    assert(shiftedBits.capacity == bits.capacity + offset)

    var i = shiftedBits.nextSetBit(0)
    for (value <- values) {
      if (value >= start) {
        assert(i == value + offset)
      } else {
        assert(i == value)
      }
      i = shiftedBits.nextSetBit(i + 1)
    }

    assert(i == -1)
  }

  it should "shift bits left more than 64 bits" in {
    val bits = new Bitset(64 * 3)
    val values = Array(0, 1, 2, 10, 11, 12, 63, 64, 66, 80, 110, 128, 129)

    for (value <- values) {
      bits.set(value)
    }

    val start = 10
    val offset = 68
    val shiftedBits = bits.shiftLeft(start, offset)
    assert(shiftedBits.capacity == bits.capacity + offset)

    var i = shiftedBits.nextSetBit(0)
    for (value <- values) {
      if (value >= start) {
        assert(i == value + offset)
      } else {
        assert(i == value)
      }
      i = shiftedBits.nextSetBit(i + 1)
    }

    assert(i == -1)
  }
}
