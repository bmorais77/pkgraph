package org.apache.spark.graphx.pkgraph.util.collection

import java.util.Arrays._

/**
  * Adapted from org.apache.spark.util.collection.BitSet
  *
  * A simple, fixed-size bit set implementation. This implementation is fast because it avoids
  * safety/bound checking.
  */
class Bitset(numBits: Int) extends Serializable {
  private val words = new Array[Long](bit2words(numBits))
  private val numWords = words.length

  /**
    * Compute the capacity (number of bits) that can be represented
    * by this bitset.
    */
  def capacity: Int = numWords * 64

  /**
    * Clear all set bits.
    */
  def clear(): Unit = fill(words, 0)

  /**
    * Set all the bits up to a given index
    */
  def setUntil(bitIndex: Int): Unit = {
    val wordIndex = bitIndex >> 6 // divide by 64
    fill(words, 0, wordIndex, -1)
    if (wordIndex < words.length) {
      // Set the remaining bits (note that the mask could still be zero)
      val mask = ~(-1L << (bitIndex & 0x3f))
      words(wordIndex) |= mask
    }
  }

  /**
    * Clear all the bits up to a given index
    */
  def clearUntil(bitIndex: Int): Unit = {
    val wordIndex = bitIndex >> 6 // divide by 64
    fill(words, 0, wordIndex, 0)
    if (wordIndex < words.length) {
      // Clear the remaining bits
      val mask = -1L << (bitIndex & 0x3f)
      words(wordIndex) &= mask
    }
  }

  /**
    * Compute the bit-wise AND of the two sets returning the
    * result.
    */
  def &(other: Bitset): Bitset = {
    val newBS = new Bitset(math.max(capacity, other.capacity))
    val smaller = math.min(numWords, other.numWords)
    assert(newBS.numWords >= numWords)
    assert(newBS.numWords >= other.numWords)
    var ind = 0
    while (ind < smaller) {
      newBS.words(ind) = words(ind) & other.words(ind)
      ind += 1
    }
    newBS
  }

  /**
    * Compute the bit-wise OR of the two sets returning the
    * result.
    */
  def |(other: Bitset): Bitset = {
    val newBS = new Bitset(math.max(capacity, other.capacity))
    assert(newBS.numWords >= numWords)
    assert(newBS.numWords >= other.numWords)
    val smaller = math.min(numWords, other.numWords)
    var ind = 0
    while (ind < smaller) {
      newBS.words(ind) = words(ind) | other.words(ind)
      ind += 1
    }
    while (ind < numWords) {
      newBS.words(ind) = words(ind)
      ind += 1
    }
    while (ind < other.numWords) {
      newBS.words(ind) = other.words(ind)
      ind += 1
    }
    newBS
  }

  /**
    * Compute the symmetric difference by performing bit-wise XOR of the two sets returning the
    * result.
    */
  def ^(other: Bitset): Bitset = {
    val newBS = new Bitset(math.max(capacity, other.capacity))
    val smaller = math.min(numWords, other.numWords)
    var ind = 0
    while (ind < smaller) {
      newBS.words(ind) = words(ind) ^ other.words(ind)
      ind += 1
    }
    if (ind < numWords) {
      Array.copy(words, ind, newBS.words, ind, numWords - ind)
    }
    if (ind < other.numWords) {
      Array.copy(other.words, ind, newBS.words, ind, other.numWords - ind)
    }
    newBS
  }

  /**
    * Compute the difference of the two sets by performing bit-wise AND-NOT returning the
    * result.
    */
  def andNot(other: Bitset): Bitset = {
    val newBS = new Bitset(capacity)
    val smaller = math.min(numWords, other.numWords)
    var ind = 0
    while (ind < smaller) {
      newBS.words(ind) = words(ind) & ~other.words(ind)
      ind += 1
    }
    if (ind < numWords) {
      Array.copy(words, ind, newBS.words, ind, numWords - ind)
    }
    newBS
  }

  /**
    * Sets the bit at the specified index to true.
    * @param index the bit index
    */
  def set(index: Int): Unit = {
    val bitmask = 1L << (index & 0x3f) // mod 64 and shift
    words(index >> 6) |= bitmask // div by 64 and mask
  }

  def unset(index: Int): Unit = {
    val bitmask = 1L << (index & 0x3f) // mod 64 and shift
    words(index >> 6) &= ~bitmask // div by 64 and mask
  }

  /**
    * Return the value of the bit with the specified index. The value is true if the bit with
    * the index is currently set in this BitSet; otherwise, the result is false.
    *
    * @param index the bit index
    * @return the value of the bit with the specified index
    */
  def get(index: Int): Boolean = {
    val bitmask = 1L << (index & 0x3f) // mod 64 and shift
    (words(index >> 6) & bitmask) != 0 // div by 64 and mask
  }

  /**
    * Get an iterator over the set bits.
    */
  def iterator: Iterator[Int] =
    new Iterator[Int] {
      private var ind = nextSetBit(0)
      override def hasNext: Boolean = ind >= 0
      override def next(): Int = {
        val tmp = ind
        ind = nextSetBit(ind + 1)
        tmp
      }
    }

  /** Return the number of bits set to true in this BitSet. */
  def cardinality(): Int = {
    var sum = 0
    var i = 0
    while (i < numWords) {
      sum += java.lang.Long.bitCount(words(i))
      i += 1
    }
    sum
  }

  /**
    * Returns the index of the first bit that is set to true that occurs on or after the
    * specified starting index. If no such bit exists then -1 is returned.
    *
    * To iterate over the true bits in a BitSet, use the following loop:
    *
    *  for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1)) {
    *    // operate on index i here
    *  }
    *
    * @param fromIndex the index to start checking from (inclusive)
    * @return the index of the next set bit, or -1 if there is no such bit
    */
  def nextSetBit(fromIndex: Int): Int = {
    var wordIndex = fromIndex >> 6
    if (wordIndex >= numWords) {
      return -1
    }

    // Try to find the next set bit in the current word
    val subIndex = fromIndex & 0x3f
    var word = words(wordIndex) >> subIndex
    if (word != 0) {
      return (wordIndex << 6) + subIndex + java.lang.Long.numberOfTrailingZeros(word)
    }

    // Find the next set bit in the rest of the words
    wordIndex += 1
    while (wordIndex < numWords) {
      word = words(wordIndex)
      if (word != 0) {
        return (wordIndex << 6) + java.lang.Long.numberOfTrailingZeros(word)
      }
      wordIndex += 1
    }

    -1
  }

  /**
    * Counts the number of bits between [start, end] that have a value of 1 in this BitSet.
    *
    * @param start Starting position (inclusive)
    * @param end   Ending position (inclusive)
    * @return number of between with value 1 between [start, end]
    */
  def count(start: Int, end: Int): Int = {
    if (start > end) {
      return 0
    }

    val startBitmask = -1L << (start & 0x3f)
    val endBitmask = if ((end + 1 & 0x3f) == 0) -1L else ~(-1L << ((end + 1) & 0x3f))

    // Special case: start and end belong to the same word
    if ((start >> 6) == (end >> 6)) {
      val bitmask = startBitmask & endBitmask
      return java.lang.Long.bitCount(words(start >> 6) & bitmask)
    }

    var sum = 0
    sum += java.lang.Long.bitCount(words(start >> 6) & startBitmask)

    // Start one ahead since we already counted the number of bits in the first word
    var currWord = (start >> 6) + 1
    val endWord = end >> 6
    while (currWord < endWord && currWord < numWords) {
      sum += java.lang.Long.bitCount(words(currWord))
      currWord += 1
    }

    // Only count the number of bits in the last word, if it is a different word then the first,
    // since the first word as already accounted for
    sum += java.lang.Long.bitCount(words(endWord) & endBitmask)
    sum
  }

  /**
    * Reverse the bits in this bitset.
    *
    * @return bitset with reversed bits
    */
  def reverse: Bitset = {
    val reversedSet = new Bitset(capacity)
    for (i <- iterator) {
      reversedSet.set(capacity - i - 1)
    }
    reversedSet
  }

  /**
    * Create a new BitSet with the same bits but with the given new capacity.
    *
    * @param newCapacity   New capacity in bits
    * @return newly created BitSet
    */
  def grow(newCapacity: Int): Bitset = {
    if (newCapacity < capacity) {
      return this
    }

    val bits = new Bitset(newCapacity)
    System.arraycopy(words, 0, bits.words, 0, words.length)
    bits
  }

  /**
    * Shifts all bits from 'start', 'offset' times to the left.
    * The empty positions will be filled with '0' values.
    * The resulting [[Bitset]] will have a new capacity of: oldCapacity + count
    *
    * @param start    Index of the bit to start shifting at (inclusive)
    * @param offset   Number of bits to shift
    * @return new bitset with bits shifted
    */
  def shiftLeft(start: Int, offset: Int): Bitset = {
    val startWord = start / 64
    val bits = new Bitset(capacity + offset)

    // Copy all words that remaining unchanged
    System.arraycopy(words, 0, bits.words, 0, startWord + 1)

    // Keep only non-shifted bits of the starting word
    val startLocal = start % 64
    val mask = -1 >>> (64 - startLocal)
    bits.words(startWord) &= mask

    // Copy remaining bits starting in 'start' with an offset of 'offset'
    var i = nextSetBit(start)
    while (i >= 0) {
      bits.set(offset + i)
      i = nextSetBit(i + 1)
    }

    bits
  }

  /** Return the number of longs it would take to hold numBits. */
  private def bit2words(numBits: Int) = ((numBits - 1) >> 6) + 1
}
