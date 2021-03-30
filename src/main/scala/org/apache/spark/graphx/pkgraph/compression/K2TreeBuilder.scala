package org.apache.spark.graphx.pkgraph.compression

import org.apache.spark.util.collection.BitSet
import K2TreeBuilder.calculateLevelOffsets
import org.apache.spark.graphx.pkgraph.util.mathx
import org.apache.spark.graphx.pkgraph.util.collection.BitSetExtensions

import scala.collection.mutable

// TODO: Add method to add individual edges and store the precomputed offsets
class K2TreeBuilder(val k: Int, val size: Int, val height: Int, val bits: BitSet, val length: Int) {
  private val k2 = k * k
  private var edgeCount: Int = 0

  /**
    * Creates a new builder with the given edges added.
    *
    * @param edges Sequence of edges to add
    * @return builder of a K²-Tree with edges added
    */
  def addEdges(edges: Seq[(Int, Int)]): K2TreeBuilder = {
    // Pre-compute the offsets of each level to the beginning of the bitset
    val offsets = calculateLevelOffsets(k, height)

    def recursiveNavigation(h: Int, line: Int, col: Int): Int = {
      if (h == 0) {
        return 0
      }

      // Offset to the beginning of this level
      val levelOffset = offsets(h)

      // Offset to the beginning of this chunk
      val chunkOffset = recursiveNavigation(h - 1, line / k, col / k)

      // Offset from the beginning of this chunk to the desired position
      val localIndex = (line % k) * k + (col % k)

      // Index relative to the beginning of the tree
      val index = levelOffset + chunkOffset + localIndex
      bits.set(index)

      chunkOffset * k2 + localIndex * k2
    }

    for ((line, col) <- edges) {
      recursiveNavigation(height, line, col)
      edgeCount += 1
    }

    this
  }

  /**
    * Creates a new builder with the given edges removed.
    *
    * @param edges Sequence of edges to remove
    * @return builder of a K²-Tree with edges removed
    */
  def removeEdges(edges: Seq[(Int, Int)]): K2TreeBuilder = {
    // Pre-compute the offsets of each level to the beginning of the bitset
    val offsets = calculateLevelOffsets(k, height)

    def tracePath(path: Array[(Int, Int)], h: Int, line: Int, col: Int): Int = {
      if (h == 0) {
        return 0
      }

      // Offset to the beginning of this level
      val levelOffset = offsets(h)

      // Offset to the beginning of this chunk
      val chunkOffset = tracePath(path, h - 1, line / k, col / k)

      // Offset from the beginning of this chunk to the desired position
      val localIndex = (line % k) * k + (col % k)

      path(h - 1) = (levelOffset + chunkOffset, levelOffset + chunkOffset + localIndex)
      chunkOffset * k2 + localIndex * k2
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

    for ((line, col) <- edges) {
      // Keeps track of the path we traversed in the tree
      val path = new Array[(Int, Int)](height)
      tracePath(path, height, line, col)
      updateBits(path)
      edgeCount -= 1
    }

    this
  }

  /**
    * Builds a compressed K²-Tree representation from this builder.
    *
    * @return K²-Tree representation
    */
  def build(): K2Tree = {
    // Count the number of bits need for the compressed version
    val (internalCount, leavesCount) = calculateCompressedSize()

    // Bitset to store both internal and leaf bits (T:L)
    val tree = new BitSet(internalCount + leavesCount)

    // Index for the tree bitset
    var t = 0

    for (i <- 0 until length / k2) {
      var include = false
      val buffer = new BitSet(k2)

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

    new K2Tree(k, size, tree, internalCount, leavesCount, edgeCount)
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
    var actualSize = size

    // Size is not a power of K, so we need to find the nearest power
    if (size % k != 0) {
      val exp = math.ceil(mathx.log(k, size))
      actualSize = math.pow(k, exp).toInt
    }

    val height = math.ceil(mathx.log(k, actualSize)).toInt
    val length = (0 to height).reduce((acc, i) => acc + math.pow(k * k, i).toInt)
    new K2TreeBuilder(k, actualSize, height, new BitSet(length), length)
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

    // Copy the first K² bits, since they belong to the first level which will be the same
    for (i <- 0 until k2) {
      if (tree.bits.get(i)) {
        builder.bits.set(i)
      }
    }

    var previousLevelCursor = 0
    var previousLevelEnd = k2
    var currentLevelCursor = k2
    var builderCursor = k2

    for (height <- 2 to builder.height) {
      // Traverse all bits of previous level
      while (previousLevelCursor < previousLevelEnd) {
        if (tree.bits.get(previousLevelCursor)) {
          // Copy bits from K²-Tree to builder
          for (i <- currentLevelCursor until currentLevelCursor + k2) {
            if (tree.bits.get(i)) {
              builder.bits.set(builderCursor)
            }
            builderCursor += 1
          }
          currentLevelCursor += k2
        } else {
          // Skipping K² bits here is the same as placing K² bits with value zero
          builderCursor += k2
        }

        // Move to next bit in previous level
        previousLevelCursor += 1
      }

      previousLevelEnd = previousLevelEnd + math.pow(k2, height).toInt
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