package org.apache.spark.graphx.pkgraph.compression

import org.apache.spark.util.collection.BitSet
import org.apache.spark.graphx.pkgraph.util.collection.BitSetExtensions

import scala.collection.mutable.ArrayBuffer

class K2Tree(
    val k: Int,
    val size: Int,
    val bits: BitSet,
    val internalCount: Int,
    val leavesCount: Int,
    val edgeCount: Int
) {

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
  def edges: Seq[(Int, Int)] = {
    val buffer = new ArrayBuffer[(Int, Int)]
    forEachEdge((line, col) => buffer += ((line, col)))
    buffer
  }

  /**
    * Returns a new K²-Tree with the given edges added.
    *
    * @param newSize Size of the adjacency matrix of the K²-Tree (i.e maximum line/col index rounded to nearest power of k)
    * @param edges   Edges to append to this K²-Tree
    * @return new K²-Tree from appending the given edges to the existing tree
    */
  def addAll(newSize: Int, edges: Array[(Int, Int)]): K2Tree = {
    K2TreeBuilder
      .fromK2Tree(grow(newSize))
      .addEdges(edges)
      .build()
  }

  /**
    * Returns a new K²-Tree with the given edges removed.
    *
    * @param edges Edges to remove from this K²-Tree
    * @return new K²-Tree with the given edges removed
    */
  def removeAll(edges: Array[(Int, Int)]): K2Tree = {
    K2TreeBuilder
      .fromK2Tree(this)
      .removeEdges(edges)
      .build()
      .trim()
  }

  /**
    * Grows this K²-Tree to the new given size.
    * All edges are kept.
    *
    * @param newSize New size to grow to
    * @return K²-Tree representing a adjacency matrix with the given new size.
    */
  def grow(newSize: Int): K2Tree = {
    if (newSize <= size) {
      return this
    }

    val k2 = k * k
    val levelChange = (size / newSize) / k2 + 1
    val internalOffset = levelChange * k2
    val bitCount = internalOffset + length

    // Prefix the tree with K² bits for every level change
    val tree = new BitSet(bitCount)
    for (i <- 0 until levelChange) {
      tree.set(i << k2)
    }

    // Add the original bits in the K²-Tree
    for (i <- 0 until length) {
      if (bits.get(i)) {
        tree.set(internalOffset + i)
      }
    }

    new K2Tree(k, newSize, tree, internalOffset + internalCount, leavesCount, edgeCount)
  }

  /**
    * Builds a new K²-Tree with the minimum required size to store all edges.
    * It is possible that the size is not changed if it's not possible to reduce the
    * size of the K²-Tree anymore.
    *
    * @return new K²-Tree with the minimum required size
    */
  def trim(): K2Tree = {
    // Cannot reduce a K²-Tree anymore than K
    if (size == k) {
      return this
    }

    val quadrantSize = size / k
    val last = quadrantSize - 1
    var shrink = true

    // Checks the last quadrants at the top level
    // If all are empty, then we can reduce the entire K²-Tree by one level
    var i = 0
    while (shrink && i < quadrantSize) {
      val lineIndex = i * quadrantSize + last
      val colIndex = last * quadrantSize + i

      if (bits.get(lineIndex) || bits.get(colIndex)) {
        shrink = false
      }

      i += 1
    }

    // Cannot shrink anymore
    if (!shrink) {
      return this
    }

    // Remove the first K² bits from the bitset
    val k2 = k * k
    val newBitCount = length - k2
    val tree = new BitSet(newBitCount)

    for (i <- 0 until newBitCount) {
      if (bits.get(k2 + i)) {
        tree.set(i)
      }
    }

    // Keep trying to trim the tree
    val newTree = new K2Tree(k, size / k, tree, internalCount - k2, leavesCount, edgeCount)
    newTree.trim()
  }

  /**
    * Iterates all edges in this tree and calls the given function for each edge.
    *
    * @param f Function called when an edge is found
    */
  def forEachEdge(f: (Int, Int) => Unit): Unit = {
    iterateEdges(f, size, 0, 0, -1)
  }

  /**
    * Reverses the order of edges in this K²-Tree
    *
    * @return K²-Tree which iterates the edges in reverse order
    */
  final def reverse: K2Tree = new ReversedK2Tree(this)

  /**
    * Recursive function to iterate through all edges in this K²-Tree.
    *
    * @param f        Function called when a edge is found
    * @param currSize Current size of the adjacency matrix
    * @param line     Line of the edge in the current level
    * @param col      Column of the edge in the current level
    * @param pos      Current position in the bitsets
    */
  protected def iterateEdges(f: (Int, Int) => Unit, currSize: Int, line: Int, col: Int, pos: Int): Unit = {
    if (pos >= internalCount) { // Is leaf node
      if (bits.get(pos)) {
        f(line, col)
        return
      }
    } else {
      if (pos == -1 || bits.get(pos)) {
        val y = rank(pos) * k * k
        val newSize = currSize / k

        for (i <- 0 until k * k) {
          iterateEdges(f, newSize, line * k + i / k, col * k + i % k, y + i)
        }
      }
    }
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
    K2TreeBuilder(k, size)
      .addEdges(edges)
      .build()
  }
}
