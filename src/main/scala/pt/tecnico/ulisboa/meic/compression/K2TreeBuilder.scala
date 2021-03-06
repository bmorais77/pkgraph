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
  def apply(k: Int, size: Int): K2TreeBuilder = empty(k, size)

  /**
   * Builds an empty K2TreeBuilder
   *
   * @param k    value of the K²-Tree
   * @param size Size of the adjacency matrix of the K²-Tree (i.e maximum line/col index rounded to nearest power of k)
   * @return empty builder for a compressed K²-Tree
   */
  def empty(k: Int, size: Int): K2TreeBuilder = {
    val height = math.ceil(mathx.log(k, size)).toInt
    val length = (0 until height).reduce((acc, i) => acc + math.pow(k * k, i).toInt)
    new K2TreeBuilder(k, size, height, new BitSet(length), length, Array.empty)
  }

  /**
   * Transforms the given K²-Tree to a builder with the given size and k value.
   *
   * @param k    New value for the K²-Tree
   * @param size New size that the builder will support
   * @param tree K²-Tree to build from
   * @return builder with the edges from this K²-Tree already added
   */
  def fromK2Tree(k: Int, size: Int, tree: K2Tree): K2TreeBuilder = {
    ???
  }
}
