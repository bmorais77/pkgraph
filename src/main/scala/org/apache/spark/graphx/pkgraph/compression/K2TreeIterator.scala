package org.apache.spark.graphx.pkgraph.compression

class K2TreeIterator(tree: K2Tree) extends Iterator[(Int, Int)] {
  private val k2 = tree.k * tree.k

  // Keeps a cursor in each level of the tree
  private val cursors = buildTreeCursors(tree)

  // Keeps the child index in each level of the tree
  private val childCursors = new Array[Int](tree.height)

  private var line: Int = 0
  private var col: Int = 0
  private var height: Int = 0

  private var currentEdge: Option[(Int, Int)] = None

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

  override def next(): (Int, Int) = {
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
    * @return next edge if there are any more edges, or [[None]] otherwise.
    */
  private def findNextEdge(): Option[(Int, Int)] = {
    var nextEdge: Option[(Int, Int)] = None

    while (nextEdge.isEmpty) {
      var nextChild = true
      val pos = cursors(height)

      // No more child nodes
      if (childCursors(height) >= k2) {
        childCursors(height) = 0
        height -= 1

        // Reached virtual root
        if (height == -1) {
          return None
        }

        line /= tree.k
        col /= tree.k
      } else {
        // Leaf node
        if (pos >= tree.internalCount) {
          if (tree.bits.get(pos)) {
            nextEdge = Some((line, col))
          }
        } else {
          if (tree.bits.get(pos)) {
            line = line * tree.k + childCursors(height + 1) / tree.k
            col = col * tree.k + childCursors(height + 1) % tree.k
            height += 1
            nextChild = false
          }
        }
      }

      if(nextChild) {
        // Move to next child node
        cursors(height) += 1
        childCursors(height) += 1

        // If the next child node is not the last
        if (childCursors(height) < k2) {
          line = line / tree.k * tree.k + childCursors(height) / tree.k
          col = col / tree.k * tree.k + childCursors(height) % tree.k
        }
      }
    }

    nextEdge
  }

  /**
    * Builds a cursor for each level of the tree.
    * Each cursor keeps track of the global position in the bitset for a given level.
    *
    * @param tree    Tree to build cursors for
    * @return cursors for each level
    */
  private def buildTreeCursors(tree: K2Tree): Array[Int] = {
    if (tree.isEmpty) {
      return Array.empty
    }

    val height = tree.height
    val cursors = new Array[Int](height)
    cursors(0) = 0 // First level cursor ia always at beginning of the bitset

    if (height > 1) {
      cursors(1) = k2 // Second level is always as an offset of K² bits
    }

    var start = 0
    var end = k2
    for (level <- 2 until height) {
      cursors(level) = cursors(level - 1) + tree.bits.count(start, end - 1) * k2
      start = end
      end = cursors(level)
    }

    cursors
  }
}
