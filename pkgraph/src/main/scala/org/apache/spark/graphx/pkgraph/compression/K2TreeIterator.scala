package org.apache.spark.graphx.pkgraph.compression

class K2TreeIterator(tree: K2Tree) extends Iterator[(Int, Int)] {
  private val k2 = tree.k * tree.k
  private val levelOffsets = tree.levelOffsets

  // Keeps a cursor in each level of the tree
  private val cursors = Array.fill(tree.height)(-1)

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
    var nextBlock = false
    var nextEdge: Option[(Int, Int)] = None

    while (nextEdge.isEmpty) {
      val offset = levelOffsets(height)
      val levelPos = cursors(height)

      val pos = tree.bits.nextSetBit(offset + levelPos + 1)

      // No more bits set to true, no more edges to find
      if (pos == -1) {
        return None
      }

      cursors(height) = pos - offset

      val prevBlockIndex = levelPos / k2
      val currBlockIndex = (pos - offset) / k2

      // We moved to another sequence of K² bits (i.e we changed parent node)
      if (!nextBlock && currBlockIndex != prevBlockIndex) {
        // Setup cursor to next block of K² child nodes
        cursors(height) = currBlockIndex * k2 - 1
        height -= 1

        // Reached virtual root
        if (height == -1) {
          return None
        }

        line /= tree.k
        col /= tree.k
      } else {
        val childIndex = (pos - offset) % k2

        // Update the line and col of the current node
        line = line / tree.k * tree.k + childIndex / tree.k
        col = col / tree.k * tree.k + childIndex % tree.k

        // Leaf node
        if (pos >= tree.internalCount) {
          nextEdge = Some((line, col))
        } else {
          // Update line and col of next child node
          line = line * tree.k + childIndex / tree.k
          col = col * tree.k + childIndex % tree.k

          height += 1
          nextBlock = true
        }
      }
    }

    nextEdge
  }
}
