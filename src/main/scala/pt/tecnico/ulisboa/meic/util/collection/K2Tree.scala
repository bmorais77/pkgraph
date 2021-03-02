package pt.tecnico.ulisboa.meic.util.collection

import org.apache.commons.lang.NotImplementedException
import org.apache.spark.util.collection.BitSet

class K2Tree(val k: Int, private val tree: BitSet, private val leaves: BitSet) {
  def ++(tree: K2Tree): K2Tree = throw new NotImplementedException()

  def iterator(): Iterator[(Int, Int)] = throw new NotImplementedException()
}

object K2Tree {
  def apply(k: Int, size: Int, edges: Array[(Int, Int)]): K2Tree = fromEdges(k, size, edges)

  // TODO: The tree bitset is being built backwards
  private def fromEdges(k: Int, size: Int, edges: Array[(Int, Int)]): K2Tree = {
    val h = math.ceil(log(k, size)).toInt
    val treeNumBits = (0 until h).reduce((acc, i) => acc + math.pow(k, 2 * i).toInt)
    val leavesNumBits = math.pow(size, 2).toInt

    val tree = new BitSet(treeNumBits)
    val leaves = new BitSet(leavesNumBits)

    for ((line, col) <- edges) {
      val index = mortonCode(line, col)
      leaves.set(index)
      buildToRoot(tree, k, h, line, col)
    }

    new K2Tree(k, tree, leaves)
  }

  /**
   * Builds a path starting at the leaf node with the given line and column and ending
   * on the root virtual node.
   *
   * If any node bit in the way to the root is already set, this function returns.
   *
   * @param tree Bitset to place node bits in
   * @param k Value of the K²-Tree
   * @param h Height of the K²-Tree
   * @param line Line of the starting leaf node
   * @param col Column of the starting leaf node
   */
  private def buildToRoot(tree: BitSet, k: Int, h: Int, line: Int, col: Int): Unit = {
    var treeLine = line
    var treeCol = col

    for(i <- h - 1 to 1) {
      treeLine /= k
      treeCol /= k

      val index = mortonCode(treeCol, treeLine)
      if (tree.get(index)) {
        return
      }

      tree.set(index)
    }
  }

  /**
   * Combines two integers using a Z-order curve (Morton Code).
   * The resulting code is an integer as only the first 16 bits of each
   * value are used.
   *
   * @param line 16-bit integer
   * @param col 16-bit integer
   * @return Morton code of 32-bit integer
   */
  private def mortonCode(line: Int, col: Int): Int = {
    var code = 0
    for (i <- 0 until 16) {
      val mask = 0x01 << i
      val x = (line & mask) << 1
      val y = col & mask
      code |= (x | y) << i
    }
    code
  }

  /**
   * Performs the logarithmic operation with the given base and value.
   *
   * log<base>(x) = log10(x) / log10(base)
   *
   * @param base Base
   * @param x Value
   * @return logarithmic result
   */
  private def log(base: Double, x: Double): Double = math.log10(x) / math.log10(base)
}
