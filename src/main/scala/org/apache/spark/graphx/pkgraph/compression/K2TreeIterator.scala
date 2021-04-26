package org.apache.spark.graphx.pkgraph.compression

import scala.collection.mutable

class K2TreeIterator(tree: K2Tree) extends Iterator[K2TreeEdge] {
  private val k2 = tree.k * tree.k

  // Keeps track of the path up to the current node
  // Note: At the top of the stack there will always be the `virtual` root node
  private val path = mutable.Stack(Node(0, 0, -1, -1))

  private var currentEdge: Option[K2TreeEdge] = None

  override def hasNext: Boolean = {
    if(tree.isEmpty) {
      return false
    }

    if (currentEdge.isDefined) {
      return true
    }

    val edge = findNextEdge
    currentEdge = edge
    edge.isDefined
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
  private def findNextEdge: Option[K2TreeEdge] = {
    if (path.isEmpty) {
      return None
    }

    val top = path.top
    top.childIndex += 1

    if (top.pos >= tree.internalCount) { // Is leaf node
      if (tree.bits.get(top.pos)) {
        val edge = K2TreeEdge(path.top.line, path.top.col)
        path.pop()
        return Some(edge)
      }
    } else {
      if (top.pos == -1 || tree.bits.get(top.pos)) {
        val y = tree.rank(top.pos) * k2

        for (i <- top.childIndex until k2) {
          val newSegment = Node(top.line * tree.k + i / tree.k, top.col * tree.k + i % tree.k, y + i, -1)

          path.push(newSegment)
          val nextEdge = findNextEdge

          if (nextEdge.isDefined) {
            return nextEdge
          }

          if(path.isEmpty) {
            return None
          }

          path.pop()
          top.childIndex += 1
        }
      }

      path.pop()
      return findNextEdge
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
