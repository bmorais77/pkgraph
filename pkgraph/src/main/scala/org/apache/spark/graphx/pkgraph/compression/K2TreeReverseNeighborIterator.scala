package org.apache.spark.graphx.pkgraph.compression

private case class K2TreeReverseNeighborCursor(subBlockStart: Int, var localIndex: Int = 0) {
  def pos(): Int = subBlockStart + localIndex
}

class K2TreeReverseNeighborIterator(tree: K2Tree, col: Int) extends Iterator[(Int, Int)] {
  private val k2 = tree.k * tree.k
  private val levelOffsets = tree.levelOffsets

  private var line: Int = 0
  private var height: Int = 0

  private var currSize: Int = tree.size / tree.k
  private var currentNeighbor: Option[(Int, Int)] = None

  // Keeps a cursor in each level of the tree
  private val cursors = new Array[K2TreeReverseNeighborCursor](tree.height)
  cursors(0) = K2TreeReverseNeighborCursor(col / currSize)

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

  override def next(): (Int, Int) = {
    currentNeighbor match {
      case Some(edge) =>
        currentNeighbor = None
        edge
      case None => throw new NoSuchElementException
    }
  }

  /**
    * Moves to the previous tree level.
    */
  private def previousLevel(): Unit = {
    currSize *= tree.k
    height -= 1
    line /= tree.k
  }

  private def findNextNeighbor(): Option[(Int, Int)] = {
    var nextNeighbor: Option[(Int, Int)] = None

    while (nextNeighbor.isEmpty) {
      // Reached virtual root
      if (height == -1) {
        return None
      }

      val levelOffset = levelOffsets(height)
      val cursor = cursors(height)
      val blockIndex = cursor.subBlockStart / k2
      val blockEnd = (blockIndex + 1) * k2 - 1

      // Look for the next bit that belongs to the same column
      var pos = cursor.pos() - 1
      do {
        pos = tree.bits.nextSetBit(pos + 1)

        // No more bits set to true, no more edges to find
        if (pos == -1) {
          return None
        }

        cursor.localIndex += pos - cursor.pos()
      } while (pos <= blockEnd && cursor.localIndex % tree.k != 0)

      if (pos > blockEnd) {
        previousLevel()
      } else {
        // Prepare next iteration of this cursor
        cursors(height).localIndex += tree.k

        val childIndex = (pos - levelOffset) % k2

        // Update the line of the current node
        line = line / tree.k * tree.k + childIndex / tree.k

        // Leaf node
        if (pos >= tree.internalCount) {
          nextNeighbor = Some(line, tree.bits.count(tree.internalCount, pos - 1))
        } else {
          // Update line of next child node
          line = line * tree.k + childIndex / tree.k
          currSize /= tree.k
          height += 1

          val nextLevelOffset = levelOffsets(height)
          val blockStart = nextLevelOffset + tree.bits.count(levelOffset, pos - 1) * k2
          val localCol = col % (currSize * tree.k)
          val subBlockStart = blockStart + localCol / currSize

          cursors(height) = K2TreeReverseNeighborCursor(subBlockStart)
        }
      }
    }

    nextNeighbor
  }
}
