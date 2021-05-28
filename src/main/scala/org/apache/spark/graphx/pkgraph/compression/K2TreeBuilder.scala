package org.apache.spark.graphx.pkgraph.compression

import org.apache.spark.graphx.pkgraph.compression.K2TreeBuilder.calculateLevelOffsets
import org.apache.spark.graphx.pkgraph.util.collection.PKBitSet
import org.apache.spark.graphx.pkgraph.util.mathx

import scala.collection.mutable

class K2TreeBuilder(val k: Int, val size: Int, val height: Int, val bits: PKBitSet, val length: Int) {
  private val k2 = k * k

  private lazy val levelOffsets = calculateLevelOffsets(k, height)

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
  def addEdge(line: Int, col: Int): Int = {
    def recursiveNavigation(currSize: Int, h: Int, line: Int, col: Int): (Int, Int) = {
      if (h == 0) {
        return (0, 0)
      }

      // Offset to the beginning of this level
      val levelOffset = levelOffsets(h)

      // Offset to the beginning of this chunk
      val (chunkOffset, subIndex) = recursiveNavigation(currSize * k2, h - 1, line / k, col / k)

      // Offset from the beginning of this chunk to the desired position
      val localIndex = (line % k) * k + (col % k)

      // Index relative to the beginning of the tree
      val index = levelOffset + chunkOffset + localIndex

      bits.set(index)
      (chunkOffset * k2 + localIndex * k2, subIndex + localIndex * currSize)
    }

    val (_, index) = recursiveNavigation(1, height, line, col)
    index
  }

  /**
    * Removes the given edge.
    * If the edge does not exist in the current builder the resulting K²-Tree will still be correct.
    *
    * @param line Line of the edge (Source)
    * @param col Column of the edge (Destination)
    * @return index of the removed edge
    */
  def removeEdge(line: Int, col: Int): Int = {
    def tracePath(path: Array[(Int, Int)], currSize: Int, h: Int, line: Int, col: Int): (Int, Int) = {
      if (h == 0) {
        return (0, 0)
      }

      // Offset to the beginning of this level
      val levelOffset = levelOffsets(h)

      // Offset to the beginning of this chunk
      val (chunkOffset, subIndex) = tracePath(path, currSize * k2, h - 1, line / k, col / k)

      // Offset from the beginning of this chunk to the desired position
      val localIndex = (line % k) * k + (col % k)

      path(h - 1) = (levelOffset + chunkOffset, levelOffset + chunkOffset + localIndex)
      (chunkOffset * k2 + localIndex * k2, subIndex + localIndex * currSize)
    }

    def updateBits(path: Array[(Int, Int)]): Unit = {
      for ((chunkOffset, index) <- path.reverseIterator) {
        bits.unset(index)

        // We are done updating if there any other bits with value 1 in the same chunk
        if (bits.count(chunkOffset, chunkOffset + k2) > 0) {
          return
        }
      }
    }

    // Keeps track of the path we traversed in the tree
    val path = new Array[(Int, Int)](height)
    val (_, index) = tracePath(path, 1, height, line, col)
    updateBits(path)
    index
  }

  /**
    * Adds the given edges.
    *
    * @param edges Sequence of edges to add
    */
  def addEdges(edges: Seq[(Int, Int)]): Unit = edges.foreach(Function.tupled(addEdge))

  /**
    * Removes the given edges.
    * If the edges do not exist in the current builder the resulting K²-Tree
    * will still be correct.
    *
    * @param edges Sequence of edges to remove
    */
  def removeEdges(edges: Seq[(Int, Int)]): Unit = edges.foreach(Function.tupled(removeEdge))

  /**
    * Builds a compressed K²-Tree representation from this builder.
    *
    * @return K²-Tree representation
    */
  def build: K2Tree = {
    // Count the number of bits need for the compressed version
    val (internalCount, leavesCount) = calculateCompressedSize()

    // Bitset to store both internal and leaf bits (T:L)
    val tree = new PKBitSet(internalCount + leavesCount)

    // Index for the tree bitset
    var t = 0

    for (i <- 0 until length / k2) {
      var include = false
      val buffer = new PKBitSet(k2)

      for (j <- 0 until k2) {
        if (bits.get(i * k2 + j)) {
          include = true
          buffer.set(j)
        }
      }

      if (include) {
        for (j <- 0 until k2) {
          if (buffer.get(j)) {
            tree.set(t)
          }
          t += 1
        }
      }
    }

    new K2Tree(k, size, tree, internalCount, leavesCount)
  }

  /**
    * Calculates the minimum size required to store all bits in the compressed representation.
    *
    * @return minimum number of bits required for the internal bits and leaves bits of the compressed representation
    */
  private def calculateCompressedSize(): (Int, Int) = {
    val leavesStart = (1 until height).map(h => math.pow(k2, h).toInt).sum / k2

    var i = 0
    var internalCount = 0
    var leavesCount = 0

    while (i < length / k2) {
      var j = 0
      var found = false

      while (!found && j < k2) {
        if (bits.get(i * k2 + j)) {
          found = true
        }
        j += 1
      }

      if (found) {
        if (i >= leavesStart) {
          leavesCount += k2
        } else {
          internalCount += k2
        }
      }

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
    if(size == 0) {
      return new K2TreeBuilder(k, 0, 0, new PKBitSet(0), 0)
    }

    // Make sure size is a power of K
    val height = math.ceil(mathx.log(k, size)).toInt
    val actualSize = math.pow(k, height).toInt

    val k2 = k * k
    val length = (0 to height).reduce((acc, i) => acc + math.pow(k2, i).toInt)
    new K2TreeBuilder(k, actualSize, height, new PKBitSet(length), length)
  }

  /**
    * Constructs a K²-Tree Builder from the given K²-Tree.
    *
    * The resulting builder will have the same size and same edges of the given K²-Tree.
    *
    * @param tree K²-Tree to construct builder from
    * @return K²-Tree builder with initial data from the given K²-Tree
    */
  def fromK2Tree(tree: K2Tree): K2TreeBuilder = {
    val k2 = tree.k * tree.k
    val builder = K2TreeBuilder(tree.k, tree.size)

    var builderCursor = -1
    var bitCursor = 0
    var treeCursor = 0

    while (treeCursor < tree.length) {
      if (builderCursor == -1 || builder.bits.get(builderCursor)) {
        // Copy next K² bits from tree
        for (i <- treeCursor until treeCursor + k2) {
          if (tree.bits.get(i)) {
            builder.bits.set(bitCursor)
          }
          bitCursor += 1
        }
        treeCursor += k2
      } else {
        // Fill builder with K² zeros
        bitCursor += k2
      }

      builderCursor += 1
    }

    builder
  }

  /**
    * Calculates the starting offsets for each level of a K²-Tree.
    *
    * @param k      Value of the K²-Tree
    * @param height Height of the K²-Tree
    * @return mapping of each level (1..height) and their corresponding starting offset.
    */
  private def calculateLevelOffsets(k: Int, height: Int): mutable.Map[Int, Int] = {
    val k2 = k * k
    val offsets = mutable.HashMap[Int, Int]()

    // Level 1 always has an offset of 0
    offsets(1) = 0

    for (i <- 2 to height) {
      offsets.put(i, offsets(i - 1) + math.pow(k2, i - 1).toInt)
    }

    offsets
  }
}
