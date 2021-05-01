package org.apache.spark.graphx.pkgraph.compression

import scala.collection.mutable

class K2TreeReverseNeighborIterator(tree: K2Tree, col: Int) extends Iterator[Int] {
  private val k2 = tree.k * tree.k

  // Keeps track of the path up to the current node
  // Note: The stack starts with the virtual node
  private val path = mutable.Stack(Node(tree.size, 0, col, -1, 0))

  private var currentNeighbor: Option[Int] = None

  override def hasNext: Boolean = {
    if (tree.isEmpty) {
      return false
    }

    if (currentNeighbor.isDefined) {
      return true
    }

    currentNeighbor = findNextNeighbor()
    currentNeighbor.isDefined
  }

  override def next(): Int = {
    currentNeighbor match {
      case Some(edge) =>
        currentNeighbor = None
        edge
      case None => throw new NoSuchElementException
    }
  }

  /**
   * Finds the next neighbor in the [[K2Tree]] for a given line, if there is any.
   * If [[hasNext]] returns true, this function is guaranteed to return a neighbor.
   *
   * @return [[Int]] with the column of the neighbor
   */
  private def findNextNeighbor(): Option[Int] = {
    while (path.nonEmpty) {
      val node = path.top
      if (node.pos >= tree.internalCount && tree.bits.get(node.pos)) { // Is non-zero leaf node
        path.pop()
        return Some(node.line)
      } else if (node.pos == -1 || tree.bits.get(node.pos)) { // Is virtual node (-1) or non-zero internal node
        val newSize = node.size / tree.k
        val y = tree.rank(node.pos) * k2 + (node.col / newSize)

        if (node.childIndex < tree.k) {
          val line = node.line + newSize * node.childIndex
          val col = node.col % newSize
          val pos = y + node.childIndex * tree.k
          val childNode = Node(newSize, line, col, pos, 0)
          path.push(childNode)
          node.childIndex += 1
        } else {
          path.pop() // All child nodes have been iterated
        }
      } else { // The node was neither a non-zero leaf node nor a non-zero internal node, so we skip it
        path.pop()
      }
    }
    None
  }

  /**
   * Case class to represent a node that has been traversed in the tree.
   * Used in the iterator to keep track of the nodes that have been traversed in the current path.
   *
   * @param size       Current size of the adjacency matrix
   * @param line       Line in the adjacency matrix
   * @param col        Column in the adjacency matrix
   * @param pos        Position in the BitSet
   * @param childIndex Index of the last child node that has been traversed
   */
  private case class Node(size: Int, line: Int, col: Int, pos: Int, var childIndex: Int)
}
