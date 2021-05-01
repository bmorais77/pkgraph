package org.apache.spark.graphx.pkgraph.compression

import scala.collection.mutable

class K2TreeIterator(tree: K2Tree) extends Iterator[K2TreeEdge] {
  private val k2 = tree.k * tree.k

  // Keeps track of the path up to the current node
  // Note: The stack starts with the virtual node
  private val path = mutable.Stack(Node(0, 0, -1, 0))

  private var currentEdge: Option[K2TreeEdge] = None

  override def hasNext: Boolean = {
    if (tree.isEmpty) {
      return false
    }

    if (currentEdge.isDefined) {
      return true
    }

    currentEdge = findNextEdge()
    currentEdge.isDefined
  }

  override def next(): K2TreeEdge = {
    currentEdge match {
      case Some(edge) =>
        currentEdge = None
        edge
      case None => throw new NoSuchElementException
    }
  }

  /**
    * Finds the next edge in the [[K2Tree]], if there is any.
    * If [[hasNext]] returns true, this function is guaranteed to return an edge.
    *
    * @return [[K2TreeEdge]] if there are any more edges, or [[None]] otherwise.
    */
  private def findNextEdge(): Option[K2TreeEdge] = {
    while (path.nonEmpty) {
      val node = path.top
      if (node.pos >= tree.internalCount && tree.bits.get(node.pos)) { // Is non-zero leaf node
        val edge = K2TreeEdge(node.line, node.col)
        path.pop()
        return Some(edge)
      } else if (node.pos == -1 || tree.bits.get(node.pos)) { // Is virtual node (-1) or non-zero internal node
        val y = tree.rank(node.pos) * k2

        // Still have child nodes to iterate
        if (node.childIndex < k2) {
          val line = node.line * tree.k + node.childIndex / tree.k
          val col = node.col * tree.k + node.childIndex % tree.k
          val pos = y + node.childIndex
          val childNode = Node(line, col, pos, 0)
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
    * @param line Line in the adjacency matrix
    * @param col Column in the adjacency matrix
    * @param pos Position in the BitSet
    * @param childIndex Index of the last child node that has been traversed
    */
  private case class Node(line: Int, col: Int, pos: Int, var childIndex: Int)
}
