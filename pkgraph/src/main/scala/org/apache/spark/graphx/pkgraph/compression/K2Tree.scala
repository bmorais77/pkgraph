package org.apache.spark.graphx.pkgraph.compression

import org.apache.spark.graphx.pkgraph.util.collection.Bitset
import org.apache.spark.graphx.pkgraph.util.mathx

class K2Tree(
    val k: Int,
    val size: Int,
    val bits: Bitset,
    val internalCount: Int,
    val leavesCount: Int,
    val leafIndices: Array[Long]
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
