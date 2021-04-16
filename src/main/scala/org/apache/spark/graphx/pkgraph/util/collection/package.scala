package org.apache.spark.graphx.pkgraph.util

import org.apache.spark.util.collection.BitSet

import scala.reflect.ClassTag

package object collection {

  implicit class BitSetExtensions(bits: BitSet) {
    /**
     * Counts the number of bits between [start, end] that have a value of 1 in this BitSet.
     *
     * @param start Starting position (inclusive)
     * @param end   Ending position (inclusive)
     * @return number of between with value 1 between [start, end]
     */
    def count(start: Int, end: Int): Int = {
      var count = 0
      for (i <- start to end) {
        if (bits.get(i)) {
          count += 1
        }
      }
      count
    }

    def reverse: BitSet = {
      val reversedSet = new BitSet(bits.capacity)
      for(i <- bits.iterator) {
        reversedSet.set(bits.capacity - i - 1)
      }
      reversedSet
    }
  }

  def splitIntoArrays[A: ClassTag, B: ClassTag](it: Iterable[(A, B)], size: Int): (Array[A], Array[B]) = {
    val first = new Array[A](size)
    val second = new Array[B](size)

    var i = 0
    for((a, b) <- it) {
      first(i) = a
      second(i) = b
      i += 1
    }

    (first, second)
  }
}
