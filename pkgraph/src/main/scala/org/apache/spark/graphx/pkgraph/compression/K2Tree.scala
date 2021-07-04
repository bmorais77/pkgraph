package org.apache.spark.graphx.pkgraph.compression

import org.apache.spark.graphx.pkgraph.util.collection.Bitset
import org.apache.spark.graphx.pkgraph.util.mathx

class K2Tree(
    val k: Int,
    val size: Int,
    val bits: Bitset,
    val internalCount: Int,
    val leavesCount: Int
) extends Serializable {

  /**
    * Total number of bits used to represent this K²-Tree.
    *
    * @return number of bits used to represent this K²-Tree
    */
  def length: Int = internalCount + leavesCount

  /**
    * Get the height of this K²-Tree
    *
    * @return height
    */
  def height: Int = if (isEmpty) 0 else math.ceil(mathx.log(k, size)).toInt

  /**
    * Returns whether this tree is empty or not.
    *
    * @return true if tree has no edges, false otherwise
    */
  def isEmpty: Boolean = leavesCount == 0

  /**
    * Collect the edges encoded in this K²-Tree
    *
    * @return sequence of edges encoded in this K²-Tree
    */
  def edges: Array[(Int, Int)] = iterator.toArray

  /**
    * Get this tree's iterator.
    *
    * @return K²-Tree iterator
    */
  def iterator: Iterator[(Int, Int)] = new K2TreeIterator(this)

  /**
    * Calls the given function for each edge stored in this K²-Tree.
    *
    * @param f   User function ((line, col) => Unit)
    */
  def foreach(f: (Int, Int) => Unit): Unit = {
    if (isEmpty) {
      return
    }

    val k2 = k * k
    def recursiveNavigation(n: Int, line: Int, col: Int, pos: Int): Unit = {
      if (pos >= internalCount) { // Is non-zero leaf node
        if (bits.get(pos)) {
          f(line, col)
        }
      } else if (pos == -1 || bits.get(pos)) { // Is virtual node (-1) or non-zero internal node
        val y = rank(pos) * k2
        val newSize = n / k

        for (i <- 0 until k2) {
          recursiveNavigation(newSize, line * k + i / k, col * k + i % k, y + i)
        }
      }
    }

    recursiveNavigation(size, 0, 0, -1)
  }

  /**
    * Rank operation of the K²-Tree.
    *
    * Counts the number of bits with value 1 in the tree bits between [0, end].
    *
    * @param end  Inclusive ending position
    * @return number of bits with value 1 between [0, end]
    */
  protected[compression] def rank(end: Int): Int = bits.count(0, end)
}

object K2Tree {

  /**
    * Builds a K²-Tree with the given k from the given edges.
    *
    * @param k     Value of the K²-Tree
    * @param size  Size of the adjacency matrix of the K²-Tree (i.e maximum line/col index rounded to nearest power of k)
    * @param edges Array of edges to build K²-Tree from
    * @return compressed K²-Tree
    */
  def apply(k: Int, size: Int, edges: Array[(Int, Int)]): K2Tree = {
    val builder = K2TreeBuilder(k, size)
    for ((line, col) <- edges) {
      builder.addEdge(line, col)
    }
    builder.build()
  }
}
