package org.apache.spark.graphx.pkgraph.compression

import scala.collection.mutable

class K2TreeIterator(tree: K2Tree) extends Iterator[(Int, Int)] {
  private val k2 = tree.k * tree.k
  private val path = mutable.Stack[PathSegment](PathSegment(0, 0, -1, -1))
  private var edgesFound = 0

  override def hasNext: Boolean = edgesFound < tree.edgeCount

  override def next(): (Int, Int) = {
    val currSize = if(path.length == 1) 1 else tree.size
    findNextEdge(currSize) match {
      case Some(edge) => edge
      case None       => throw new NoSuchElementException
    }
  }

  private def findNextEdge(size: Int): Option[(Int, Int)] = {
    if (path.isEmpty) {
      return None
    }

    val top = path.top
    top.childIndex += 1

    if (top.pos >= tree.internalCount) { // Is leaf node
      if (tree.bits.get(top.pos)) {
        path.pop()
        edgesFound += 1
        return Some((top.line, top.col))
      }
    } else {
      if (top.pos == -1 || tree.bits.get(top.pos)) {
        val y = tree.rank(top.pos) * k2

        for (i <- top.childIndex until k2) {
          val newSegment = PathSegment(top.line * tree.k + i / tree.k, top.col * tree.k + i % tree.k, y + i, -1)

          path.push(newSegment)
          val nextEdge = findNextEdge(size / tree.k)

          if (nextEdge.isDefined) {
            return nextEdge
          }

          path.pop()
          top.childIndex += 1
        }
      }

      path.pop()
      return findNextEdge(size * tree.k)
    }
    None
  }

  private case class PathSegment(line: Int, col: Int, pos: Int, var childIndex: Int)
}
