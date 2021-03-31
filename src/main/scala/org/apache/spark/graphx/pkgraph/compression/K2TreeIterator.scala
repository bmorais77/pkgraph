package org.apache.spark.graphx.pkgraph.compression

import scala.collection.mutable

class K2TreeIterator(tree: K2Tree, reverse: Boolean = false) extends Iterator[(K2TreeIndex, Int, Int)] {
  private val k2 = tree.k * tree.k
  private val path = mutable.Stack(Node(0, 0, -1, -1))
  private var edgesFound = 0

  override def hasNext: Boolean = edgesFound < tree.edgeCount

  override def next(): (K2TreeIndex, Int, Int) = {
    findNextEdge match {
      case Some(edge) => edge
      case None       => throw new NoSuchElementException
    }
  }

  private def findNextEdge: Option[(K2TreeIndex, Int, Int)] = {
    if (path.isEmpty) {
      return None
    }

    val top = path.top
    top.childIndex += 1

    if (top.pos >= tree.internalCount) { // Is leaf node
      if (tree.bits.get(top.pos)) {
        val index = buildEdgeIndexFromPath()
        path.pop()
        edgesFound += 1
        return Some(index, top.line, top.col)
      }
    } else {
      if (top.pos == -1 || tree.bits.get(top.pos)) {
        val y = tree.rank(top.pos) * k2

        for (i <- indices(top)) {
          val newSegment = Node(top.line * tree.k + i / tree.k, top.col * tree.k + i % tree.k, y + i, -1)

          path.push(newSegment)
          val nextEdge = findNextEdge

          if (nextEdge.isDefined) {
            return nextEdge
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

  private def indices(node: Node): Range = {
    if (reverse)
      (k2 - node.childIndex - 1) to 0 by -1
    else
      node.childIndex until k2
  }

  private def buildEdgeIndexFromPath(): K2TreeIndex = {
    val indices = new Array[Int](path.length - 1)
    for (i <- 1 until path.length) {
      indices(i - 1) = (path(i).line % tree.k) * tree.k + (path(i).col % tree.k)
    }
    K2TreeIndex(indices)
  }

  private case class Node(line: Int, col: Int, pos: Int, var childIndex: Int)
}
