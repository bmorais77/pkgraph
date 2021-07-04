package org.apache.spark.graphx.pkgraph.compression

import org.apache.spark.graphx.pkgraph.util.collection.Bitset
import org.apache.spark.graphx.pkgraph.util.mathx

class K2TreeBuilder(val k: Int, val size: Int, val height: Int) {
  private val cursors = Array.fill[K2TreeCursor](height)(K2TreeCursor())
  private val k2 = k * k

  /**
    * Adds the edge with the given line and column to the this builder.
    * Returns the position that the edge was inserted in.
    *
    * Note: The returned position should not be used as the actual position of the set bit, since
    * when the final K²-Tree is built the position may be different, should only be use for ordering edges.
    *
    * @param line Edge line (source)
    * @param col Edge column (destination)
    * @return index of the inserted edge
    */
  def addEdge(line: Int, col: Int): Unit = {
    var i = cursors.length - 1
    var currLine = line
    var currCol = col
    var parentSize = size / k

    while (i >= 0) {
      // Current level cursor
      val cursor = cursors(i)

      // Index of the node relative to parent node
      val childIndex = (currLine % k) * k + (currCol % k)

      // Index of the parent node relative to its level
      val parentIndex = (currLine / k) * parentSize + (currCol / k)

      // Offset to start of child nodes of the current parent
      // We skip to the next K² if we are now in a different parent node then the current cursor
      var parentOffset = (cursor.sentinel / k2) * k2
      if (cursor.parentIndex != -1 && cursor.parentIndex != parentIndex) {
        parentOffset += k2
      }

      // Index relative to beginning of this level
      val levelIndex = parentOffset + childIndex

      // Bitset has no more space, we need to grow
      while (levelIndex >= cursor.bits.capacity) {
        cursor.bits = cursor.bits.grow(cursor.bits.capacity * 2)
      }

      // Path up to this bit is already built, we can leave now
      if (cursor.bits.get(levelIndex)) {
        return
      }

      cursor.bits.set(levelIndex)
      cursor.parentIndex = parentIndex
      cursor.sentinel = levelIndex

      currLine /= k
      currCol /= k
      parentSize /= k
      i -= 1
    }
  }

  /**
    * Builds a compressed K²-Tree representation from this builder.
    *
    * @return K²-Tree representation
    */
  def build(): K2Tree = {
    val (internalCount, leavesCount) = calculateCompressedSize()

    var offset = 0
    val bits = new Bitset(internalCount + leavesCount)

    var i = 0
    while (i < cursors.length) {
      val cursor = cursors(i)

      // Fill the K²-Tree bits with the bits of each level's Bitset
      var pos = cursor.bits.nextSetBit(0)
      while (pos >= 0 && pos <= cursor.sentinel) {
        bits.set(offset + pos)
        pos = cursor.bits.nextSetBit(pos + 1)
      }

      offset += (cursor.sentinel / k2) * k2 + k2
      i += 1
    }

    new K2Tree(k, size, bits, internalCount, leavesCount)
  }

  /**
    * Calculates the minimum size required to store all bits in the compressed representation.
    *
    * @return minimum number of bits required for the internal bits and leaves bits of the compressed representation
    */
  private def calculateCompressedSize(): (Int, Int) = {
    // Calculate leaf nodes count
    val leavesCount = (cursors(cursors.length - 1).sentinel / k2) * k2 + k2

    // Calculate internal nodes count
    var i = 0
    var internalCount = 0
    while (i < cursors.length - 1) {
      internalCount += (cursors(i).sentinel / k2) * k2 + k2
      i += 1
    }

    (internalCount, leavesCount)
  }
}

object K2TreeBuilder {

  /**
    * Builds an empty K2TreeBuilder
    * If the given size is not a power of k, the nearest power will be used.
    *
    * @param k    value of the K²-Tree
    * @param size Size of the adjacency matrix of the K²-Tree (i.e maximum line/col index rounded to nearest power of k)
    * @return empty builder for a compressed K²-Tree
    */
  def apply(k: Int, size: Int): K2TreeBuilder = {
    // Special case where we are asked to build an empty tree
    if (size == 0) {
      return new K2TreeBuilder(0, 0, 0)
    }

    // Make sure size is a power of K
    val height = math.ceil(mathx.log(k, size)).toInt
    val actualSize = math.pow(k, height).toInt

    new K2TreeBuilder(k, actualSize, height)
  }
}
