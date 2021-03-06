package pt.tecnico.ulisboa.meic.compression

import org.apache.spark.util.collection.BitSet
import pt.tecnico.ulisboa.meic.util.collection.BitSetExtensions

class K2Tree(val k: Int, val matrixSize: Int, val internalBits: Int, val internal: BitSet, val leaves: BitSet) extends Iterable[(Int, Int)] {
  def iterator(): Iterator[(Int, Int)] = {
    val edges = collectEdges(matrixSize, 0, 0, -1)
    edges.iterator
  }

  /**
   * Builds a new K2-Tree by appending the edges to an existing tree.
   *
   * @param newSize Size of the adjacency matrix of the K²-Tree (i.e maximum line/col index rounded to nearest power of k)
   * @param edges   Edges to build K²-Tree from
   * @return new K²-Tree from appending the given edges to the existing tree
   */
  def append(newSize: Int, edges: Array[(Int, Int)]): K2Tree = {
    K2TreeBuilder
      .empty(k, newSize)
      .withEdges(toArray ++ edges)
      .build()
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
    if (pos >= internalBits) { // Is leaf node
      if (leaves.get(pos - internalBits)) {
        return (line, col) :: Nil
      }
    } else {
      if (pos == -1 || internal.get(pos)) {
        val y = rank(internal, pos) * k * k
        val newSize = currSize / k
        return (0 until k * k)
          .flatMap(i => collectEdges(newSize, line * currSize + i / k, col * currSize + i % k, y + i))
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
      .withEdges(edges)
      .build()
  }
}
