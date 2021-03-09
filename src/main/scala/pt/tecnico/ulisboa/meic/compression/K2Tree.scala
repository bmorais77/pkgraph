package pt.tecnico.ulisboa.meic.compression

import org.apache.spark.util.collection.BitSet
import pt.tecnico.ulisboa.meic.util.collection.BitSetExtensions

import scala.collection.mutable.ArrayBuffer

class K2Tree(val k: Int, val size: Int, val bits: BitSet, val internalCount: Int, val leavesCount: Int) {
  /**
   * Total number of bits used to represent this K²-Tree.
   *
   * @return number of bits used to represent this K²-Tree
   */
  def length: Int = internalCount + leavesCount

  /**
   * Collect the edges encoded in this K²-Tree
   *
   * @return sequence of edges encoded in this K²-Tree
   */
  def edges: Seq[(Int, Int)] = collectEdges(size, 0, 0, -1)

  /**
   * Builds a new K2-Tree by appending the edges to an existing tree.
   *
   * @param newSize Size of the adjacency matrix of the K²-Tree (i.e maximum line/col index rounded to nearest power of k)
   * @param edges   Edges to build K²-Tree from
   * @return new K²-Tree from appending the given edges to the existing tree
   */
  def addAll(newSize: Int, edges: Array[(Int, Int)]): K2Tree = {
    K2TreeBuilder
      .fromK2Tree(grow(newSize))
      .addEdges(edges)
      .build()
  }

  def removeAll(edges: Array[(Int, Int)]): K2Tree = {
    K2TreeBuilder
      .fromK2Tree(this)
      .removeEdges(edges)
      .build()
  }

  /**
   * Grows this K²-Tree to the new given size.
   * All edges are kept.
   *
   * @param newSize New size to grow to
   * @return K²-Tree representing a adjacency matrix with the given new size.
   */
  def grow(newSize: Int): K2Tree = {
    if(newSize <= size) {
      return this
    }

    val k2 = k * k
    val levelChange = (size / newSize) / k2 + 1
    val internalOffset = levelChange * k2
    val bitCount = internalOffset + length

    // Prefix the tree with K² bits for every level change
    val tree = new BitSet(bitCount)
    for(i <- 0 until levelChange) {
      tree.set(i << k2)
    }

    // Add the original bits in the K²-Tree
    for(i <- 0 until length) {
      if(bits.get(i)) {
        tree.set(internalOffset + i)
      }
    }

    new K2Tree(k, newSize, tree, internalOffset + internalCount, leavesCount)
  }

  def shrink(newSize: Int): K2Tree = {
    ???
  }

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
   * Recursive function to collect all edges in this K²-Tree.
   *
   * @param currSize Current size of the adjacency matrix
   * @param line     Line of the edge in the current level
   * @param col      Column of the edge in the current level
   * @param pos      Current position in the bitsets
   * @return sequence of edges
   */
  private def collectEdges(currSize: Int, line: Int, col: Int, pos: Int): Seq[(Int, Int)] = {
    if (pos >= internalCount) { // Is leaf node
      if (bits.get(pos)) {
        return (line, col) :: Nil
      }
    } else {
      if (pos == -1 || bits.get(pos)) {
        val y = rank(bits, pos) * k * k
        val newSize = currSize / k
        val edges = new ArrayBuffer[(Int, Int)](k * k)

        for(i <- 0 until k * k) {
          edges ++= collectEdges(newSize, line * k + i / k, col * k + i % k, y + i)
        }

        return edges
      }
    }
    Nil
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
    K2TreeBuilder(k, size)
      .addEdges(edges)
      .build()
  }
}
