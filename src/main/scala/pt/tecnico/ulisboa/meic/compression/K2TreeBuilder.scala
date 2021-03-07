package pt.tecnico.ulisboa.meic.compression

import org.apache.spark.util.collection.BitSet
import pt.tecnico.ulisboa.meic.util.collection.BitSetExtensions
import pt.tecnico.ulisboa.meic.util.{hash, mathx}

import scala.annotation.tailrec
import scala.collection.mutable

private class K2TreeBuilder(val k: Int, val size: Int, val height: Int, val bits: BitSet, val length: Int, val edges: Array[(Int, Int)]) {
  private val k2 = k * k

  /**
   * Creates a new builder with the given edges added.
   *
   * @param edges Array of edges to add
   * @return builder of a K²-Tree with edges added
   */
  def withEdges(edges: Array[(Int, Int)]): K2TreeBuilder = {
    def recursiveBuild(h: Int, line: Int, col: Int): Int = {
      // Virtual root node
      if (h == 0) {
        return 0
      }

      // Build the upper levels first to get the offset
      val levelOffset = recursiveBuild(h - 1, line / k, col / k)

      // Index relative to the current level
      val levelIndex = hash.mortonCode(line, col)

      // Index relative to the beginning of the tree
      val treeIndex = levelOffset + levelIndex
      bits.set(treeIndex)

      // Because the representation is uncompressed there are always K² child nodes.
      // Total number of children varies according to the current level K^{2h}
      levelOffset + math.pow(k2, h).toInt
    }

    for ((line, col) <- edges) {
      // Skip leaf nodes level, since we only need to pre-compute the internal nodes
      recursiveBuild(height - 1, line / k, col / k)
    }

    new K2TreeBuilder(k, size, height, bits, length, this.edges ++ edges)
  }

  /**
   * Calculates the offset from the beginning of the tree to each level.
   *
   * @return mapping of the offset of each level
   */
  def calculateLevelOffsets(): mutable.Map[Int, Int] = {
    val offsets = mutable.HashMap[Int, Int]()

    var levelStart = 0
    var levelEnd = 0

    // Virtual level h=0
    offsets(0) = 0

    // First level always has a offset of zero
    offsets(1) = 0

    for (h <- 1 until height) {
      if (h == 1) {
        // h=1 will always have K² bits unless the tree is completely empty
        offsets(h + 1) = k2
      } else {
        val accOffset = offsets(h)
        val levelWidth = bits.count(levelStart, levelEnd - 1) * k2
        offsets(h + 1) = accOffset + levelWidth
      }

      levelStart = levelEnd
      levelEnd = levelStart + math.pow(k2, h).toInt
    }

    offsets
  }

  /**
   * Calculates the minimum size required to store all bits in the compressed representation.
   *
   * @return minimum number of bits required for the internal bits of the compressed representation
   */
  def calculateCompressedSize(): Int = {
    var i = 0
    var count = 0

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
        count += k2
      }

      i += 1
    }

    count
  }

  /**
   * Builds a compressed K²-Tree representation from this builder.
   *
   * @return K²-Tree representation
   */
  def build(): K2Tree = {
    // Count the number of bits need for the compressed version
    val internalNumBits = calculateCompressedSize()

    // The actual number of necessary bits may be smaller than estimated here, since multiple
    // edges can belong to the same parent node
    val leavesNumBits = edges.length * k2

    // Calculates the offset from the beginning of the tree to the start of each level
    val offsets = calculateLevelOffsets()

    // TODO: Maybe use a single BitSet to represent all bits
    val internal = new BitSet(internalNumBits)
    val leaves = new BitSet(leavesNumBits)

    @tailrec
    def recursiveBuildToRoot(h: Int, line: Int, col: Int): Unit = {
      // Virtual root node
      if (h == 0) {
        return
      }

      val previousLevelOffset = offsets(h - 1)
      val levelOffset = offsets(h)

      val previousLevelIndex = hash.mortonCode(line / k, col / k)
      val localOffset = bits.count(previousLevelOffset, previousLevelOffset + previousLevelIndex - 1) * k2
      val index = levelOffset + localOffset + hash.mortonCode(line % k, col % k)

      if (index >= internalNumBits) {
        leaves.set(index - internalNumBits)
      } else {
        // Path was already built by another edge, we can leave now
        if (internal.get(index)) {
          return
        }
        internal.set(index)
      }

      recursiveBuildToRoot(h - 1, line / k, col / k)
    }

    for ((line, col) <- edges) {
      recursiveBuildToRoot(height, line, col)
    }

    new K2Tree(k, size, internalNumBits, internal, leaves)
  }
}

private object K2TreeBuilder {
  /**
   * Builds an empty K2TreeBuilder
   *
   * @param k    value of the K²-Tree
   * @param size Size of the adjacency matrix of the K²-Tree (i.e maximum line/col index rounded to nearest power of k)
   * @return empty builder for a compressed K²-Tree
   */
  def apply(k: Int, size: Int): K2TreeBuilder = {
    val height = math.ceil(mathx.log(k, size)).toInt
    val length = (0 until height).reduce((acc, i) => acc + math.pow(k * k, i).toInt)
    new K2TreeBuilder(k, size, height, new BitSet(length), length, Array.empty)
  }

  /**
   * TODO: Not sure if this is faster than simply creating an empty builder and adding all edges at once
   *
   * Transforms the given K²-Tree to a builder with the given new size.
   *
   * The new size of the tree can be larger than the original size, in
   * that case we need to prefix the T bitset with K² bits for every level
   * change:
   *
   * Example of growing from 4x4 to 8x8
   *
   * Matrix 4x4:
   * +---+---+---+---+
   * | 0   0   0   0 |
   * | 1   1   0   0 |
   * | 0   1   0   0 |
   * | 1   0   0   0 |
   * +---+---+---+---+
   *
   * T: 1010
   * L: 0011 0110
   *
   * Matrix 8x8:
   * +---+---+---+---+---+---+---+---+
   * | 0   0   0   0   0   0   0   0 |
   * | 1   1   0   0   0   0   0   0 |
   * | 0   1   0   0   0   0   0   0 |
   * | 1   0   0   0   0   0   0   0 |
   * | 0   0   0   0   0   0   0   0 |
   * | 0   0   0   0   0   0   0   0 |
   * | 0   0   0   0   0   0   0   0 |
   * | 0   0   0   0   0   0   0   0 |
   * +---+---+---+---+---+---+---+---+
   *
   * T: 1000 1010
   * L: 0011 0110
   *
   * The bits 1000 were prefix to the T bitset because there is only 1 level change
   * between size 4 and 8.
   *
   * @param size New size that the builder will support
   * @param tree K²-Tree to build from
   * @return builder with the edges from this K²-Tree already added
   */
  def fromK2Tree(size: Int, tree: K2Tree): K2TreeBuilder = {
    val k2 = tree.k * tree.k
    val levelChange = if(size > tree.matrixSize) (tree.matrixSize / size) / k2 + 1 else 0
    val internalOffset = levelChange * k2
    val internalBitsCount = internalOffset + tree.internalBits

    // Prefix the internal bits with K² for every level change
    val internal = new BitSet(internalBitsCount)
    for(i <- 0 until levelChange) {
      internal.set(i << k2)
    }

    // Add the original bits in the K²-Tree
    for(i <- 0 until tree.internalBits) {
      if(tree.internal.get(i)) {
        internal.set(internalOffset + i)
      }
    }

    val builder = K2TreeBuilder(tree.k, size)
    val edges = mutable.ArrayBuffer[(Int, Int)]()

    def buildRecursive(height: Int, currSize: Int, line: Int, col: Int, pos: Int): Unit = {
      if (pos >= internalBitsCount) { // Is leaf node
        if (tree.leaves.get(pos - internalBitsCount)) {
          edges += line -> col
          return
        }
      } else {
        if (pos == -1 || internal.get(pos)) {
          if(pos != -1) {
            val previousLevelIndex = hash.mortonCode(line / tree.k, col / tree.k)
            val levelOffset = previousLevelIndex * k2
            builder.bits.set(levelOffset + pos)
          }

          val y = internal.count(0, pos) * k2
          val newSize = currSize / tree.k

          for (i <- 0 until k2) {
            buildRecursive(height + 1, newSize, line * currSize + i / tree.k, col * currSize + i % tree.k, y + i)
          }
        }
      }
    }

    buildRecursive(0, size, 0, 0, -1)
    new K2TreeBuilder(tree.k, size, builder.height, builder.bits, builder.length, edges.toArray)
  }
}
