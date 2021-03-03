package pt.tecnico.ulisboa.meic.util.collection

import org.apache.spark.util.collection.BitSet
import pt.tecnico.ulisboa.meic.util.{hash, mathx}

class K2Tree(val k: Int, val matrixSize: Int, val internalBits: Int, val internal: BitSet, val leaves: BitSet) extends Iterable[(Int, Int)] {
  private val k2 = math.pow(k, 2).toInt

  def edges: Seq[(Int, Int)] = collectEdges(matrixSize, 0, 0, -1)

  def iterator(): Iterator[(Int, Int)] = edges.iterator

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

  private def rank(bits: BitSet, endInclusive: Int): Int = {
    var count = 0
    for (i <- 0 to endInclusive) {
      if (bits.get(i)) {
        count += 1
      }
    }
    count
  }

  private def fromEdges(k: Int, size: Int, edges: Array[(Int, Int)]): K2Tree = {
    val h = math.ceil(mathx.log(k, size)).toInt
    val internalNumBits = (0 until h).reduce((acc, i) => acc + math.pow(k, 2 * i).toInt)
    val leavesNumBits = math.pow(size, 2).toInt

    val internal = new BitSet(internalNumBits)
    val leaves = new BitSet(leavesNumBits)

    for ((line, col) <- edges) {
      val index = hash.mortonCode(line, col)
      leaves.set(index)
      buildToRoot(internal, k, size, h, line, col)
    }

    new K2Tree(k, size, internalNumBits, internal, leaves)
  }

  /**
   * Builds a path starting at the leaf node with the given line and column and ending
   * on the root virtual node.
   *
   * If any node bit in the way to the root is already set, this function returns.
   *
   * @param tree Bitset to place node bits in
   * @param k    Value of the K²-Tree
   * @param size Size of the adjacency matrix
   * @param h    Height of the K²-Tree
   * @param line Line of the starting leaf node
   * @param col  Column of the starting leaf node
   */
  private def buildToRoot(tree: BitSet, k: Int, size: Int, h: Int, line: Int, col: Int): Unit = {
    var treeLine = line
    var treeCol = col
    var treeSize = size

    for (_ <- h - 1 to 1) {
      treeLine /= k
      treeCol /= k
      treeSize /= k

      val index = hash.mortonCode(treeLine, treeCol)
      if (tree.get(index)) {
        return
      }

      tree.set(index)
    }
  }
}
