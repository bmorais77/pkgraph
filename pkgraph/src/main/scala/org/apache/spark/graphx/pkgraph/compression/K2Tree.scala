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

  if (size > (1L << 31)) {
    throw new Exception(s"K²-Tree matrix size must be smaller than 2^31 (size: $size)")
  }

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
   * Get an iterator to find all direct neighbors of the given line.
   *
   * @param line    Line to look for direct neighbors
   * @return direct neighbor iterator returning the neighbor's column and leaf index
   */
  def directNeighborIterator(line: Int): Iterator[(Int, Int)] = new K2TreeDirectNeighborIterator(this, line)

  /**
   * Get an iterator to find all reverse neighbors of the given column.
   *
   * @param column    Column to look for reverse neighbors
   * @return reverse neighbor iterator returning the neighbor's line and leaf index
   */
  def reverseNeighborIterator(column: Int): Iterator[(Int, Int)] = new K2TreeReverseNeighborIterator(this, column)

  /**
   * Returns an array containing the offset in the tree's bitset of each level.
   *
   * @return offsets for each level
   */
  def levelOffsets: Array[Int] = {
    if (isEmpty) {
      return Array.empty
    }

    val k2 = k * k
    val computedHeight = height
    val offsets = new Array[Int](computedHeight)
    offsets(0) = 0 // First level is always at beginning of the bitset

    if (computedHeight > 1) {
      offsets(1) = k2 // Second level is always as an offset of K² bits
    }

    var start = 0
    var end = k2
    for (level <- 2 until computedHeight) {
      offsets(level) = offsets(level - 1) + bits.count(start, end - 1) * k2
      start = end
      end = offsets(level)
    }

    offsets
  }
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
