package pt.tecnico.ulisboa.meic.compression

import org.apache.spark.util.collection.BitSet
import pt.tecnico.ulisboa.meic.util.collection.BitSetExtensions
import pt.tecnico.ulisboa.meic.util.{hash, mathx}

class K2Tree(val k: Int, val matrixSize: Int, val internalBits: Int, val internal: BitSet, val leaves: BitSet) extends Iterable[(Int, Int)] {
  private val k2 = math.pow(k, 2).toInt

  def iterator(): Iterator[(Int, Int)] = {
    val edges = collectEdges(matrixSize, 0, 0, -1)
    edges.iterator
  }

  def append(newSize: Int, edges: Array[(Int, Int)]): K2Tree = K2Tree.appendEdges(this, newSize, edges)

  private def collectEdges(currSize: Int, line: Int, col: Int, pos: Int): Seq[(Int, Int)] = {
    if (pos >= internalBits) { // Is leaf node
      if (leaves.get(pos - internalBits)) {
        return (line, col) :: Nil
      }
    } else {
      if (pos == -1 || internal.get(pos)) {
        val y = K2Tree.rank(internal, pos) * k2
        val newSize = currSize / k
        return (0 until k2)
          .flatMap(i => collectEdges(newSize, line * currSize + i / k, col * currSize + i % k, y + i))
      }
    }
    Nil
  }
}

object K2Tree {
  def apply(k: Int, size: Int, edges: Array[(Int, Int)]): K2Tree = fromEdges(k, size, edges)

  /**
   * Rank operation of the K²-Tree.
   *
   * Counts the number of bits with value 1 in the given BitSet between [0, end].
   *
   * @param bits BitSet to check bits
   * @param end  Inclusive ending position
   * @return number of bits with value 1 between [0, end]
   */
  private def rank(bits: BitSet, end: Int): Int = bits.count(0, end)

  /**
   * Builds a new K2-Tree from the given edges.
   *
   * @param k     Value of the new K²-Tree
   * @param size  Size of the adjacency matrix of the K²-Tree (i.e maximum line/col index rounded to nearest power of k)
   * @param edges Edges to build K²-Tree from
   * @return new K²-Tree from edges
   */
  private def fromEdges(k: Int, size: Int, edges: Array[(Int, Int)]): K2Tree = {
    val height = math.ceil(mathx.log(k, size)).toInt
    val maxInternalBits = (0 until height).reduce((acc, i) => acc + math.pow(k * k, i).toInt)
    val uncompressed = buildUncompressed(k, height, maxInternalBits, edges)
    compress(k, uncompressed, maxInternalBits, height, size, edges)
  }

  /**
   * Builds a new K2-Tree by appending the edges to an existing tree.
   *
   * @param existingTree Optional existing tree to append edges to
   * @param newSize      Size of the adjacency matrix of the K²-Tree (i.e maximum line/col index rounded to nearest power of k)
   * @param edges        Edges to build K²-Tree from
   * @return new K²-Tree from appending the given edges to the existing tree
   */
  private def appendEdges(existingTree: K2Tree, newSize: Int, edges: Array[(Int, Int)]): K2Tree = {
    ??? // TODO
  }

  /**
   * Compresses the given uncompressed representation into the final representation of the K²-Tree.
   *
   * @param k Value of the K²-Tree
   * @param uncompressed Uncompressed representation
   * @param length Length in bits of the uncompressed representation
   * @param height Height of the K²-Tree
   * @param size Size of the adjacency matrix of the K²-Tree (i.e maximum line/col index rounded to nearest power of k)
   * @param edges Array of edges to place in the K²-Tree
   * @return compressed K²-Tree
   */
  private def compress(k: Int, uncompressed: BitSet, length: Int, height: Int, size: Int, edges: Array[(Int, Int)]): K2Tree = {
    val k2 = k * k

    // Count the number of bits need for the compressed version
    val internalNumBits = calculateCompressedSize(k, uncompressed, length)

    // The actual number of necessary bits may be smaller than estimated here, since multiple
    // edges can belong to the same parent node
    val leavesNumBits = edges.length * k * k

    // TODO: Maybe use a single BitSet to represent all bits
    val internal = new BitSet(internalNumBits)
    val leaves = new BitSet(leavesNumBits)

    def recursiveBuildToRoot(h: Int, line: Int, col: Int): (Int, Int) = {
      // Virtual root node
      if (h == 0) {
        return (0, 0)
      }

      val (levelOffset, previousLevelOffset) = recursiveBuildToRoot(h - 1, line / k, col / k)
      val previousLevelIndex = hash.mortonCode(line / k, col / k)
      val localOffset = uncompressed.count(previousLevelOffset, previousLevelOffset + previousLevelIndex - 1) * k2
      val index = levelOffset + localOffset + hash.mortonCode(line % k, col % k)

      if (index >= internalNumBits) {
        leaves.set(index - internalNumBits)
      } else {
        internal.set(index)
      }

      // h=1 is a special case because the previous level is virtual
      // Unless the tree is completely empty, level h=1 is always have k²
      if (h == 1) {
        return (k2, levelOffset)
      }

      // Index of where the current level ends in the uncompressed representation
      val levelEndIndex = math.pow(k2, h - 1).toInt - 1
      val levelSkip = uncompressed.count(previousLevelOffset, previousLevelOffset + levelEndIndex) * k2
      (levelOffset + levelSkip, levelOffset)
    }

    for ((line, col) <- edges) {
      recursiveBuildToRoot(height, line, col)
    }

    new K2Tree(k, size, internalNumBits, internal, leaves)
  }

  /**
   * Builds an uncompressed version of a K²-Tree (i.e sequences of K² bits of value zero are kept in the bitsets).
   *
   * This representation is only used as an auxiliary structure to build the final compressed K²-Tree representation.
   *
   * @param k      Value of the K²-Tree
   * @param height Height of the K²-Tree
   * @param length Number of bits for the internal nodes
   * @param edges  Array of edges to add to K²-Tree
   * @return uncompressed version of a K²-Tree and the number of internal bits necessary for the compressed version
   */
  private def buildUncompressed(k: Int, height: Int, length: Int, edges: Array[(Int, Int)]): BitSet = {
    val k2 = k * k
    val tree = new BitSet(length)

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
      tree.set(treeIndex)

      // Because the representation is uncompressed there are always K² child nodes.
      // Total number of children varies according to the current level K^{2h}
      levelOffset + math.pow(k2, h).toInt
    }

    for ((line, col) <- edges) {
      // Skip leaf nodes level, since we only need to pre-compute the internal nodes
      recursiveBuild(height - 1, line / k, col / k)
    }

    tree
  }

  /**
   * Calculates the minimum size required to store all bits in the compressed
   * representation from the given uncompressed representation.
   *
   * @param k            Value of the K²-Tree
   * @param uncompressed Uncompressed representation
   * @param length       Length in bits of the uncompressed representation
   * @return minimum number of bits required for the internal bits of the compressed representation
   */
  def calculateCompressedSize(k: Int, uncompressed: BitSet, length: Int): Int = {
    val k2 = k * k
    var i = 0
    var count = 0

    while (i < length / k2) {
      var j = 0
      var found = false

      while (!found && j < k2) {
        if (uncompressed.get(i * k2 + j)) {
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
}
