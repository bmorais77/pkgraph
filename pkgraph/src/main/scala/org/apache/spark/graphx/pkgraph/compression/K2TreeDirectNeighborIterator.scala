package org.apache.spark.graphx.pkgraph.compression

private case class K2TreeDirectNeighborCursor(subBlockStart: Int, var localIndex: Int = 0) {
  def pos(): Int = subBlockStart + localIndex
}

class K2TreeDirectNeighborIterator(tree: K2Tree, line: Int) extends Iterator[(Int, Int)] {
  private val k2 = tree.k * tree.k
  private val levelOffsets = tree.levelOffsets

  private var col: Int = 0
  private var height: Int = 0

  private var currSize: Int = tree.size / tree.k
  private var currentNeighbor: Option[(Int, Int)] = None

  // Keeps a cursor in each level of the tree
  private val cursors = new Array[K2TreeDirectNeighborCursor](tree.height)
  cursors(0) = K2TreeDirectNeighborCursor((line / currSize) * tree.k)

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
    col /= tree.k
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
      val pos = tree.bits.nextSetBit(cursor.pos())

      // No more bits set to true, no more edges to find
      if (pos == -1) {
        return None
      }

      cursor.localIndex += pos - cursor.pos()
      if (cursor.localIndex >= tree.k) {
        previousLevel()
      } else {
        // Prepare next iteration of this cursor
        cursors(height).localIndex += 1

        val childIndex = (pos - levelOffset) % k2

        // Update the col of the current node
        col = col / tree.k * tree.k + childIndex % tree.k

        // Leaf node
        if (pos >= tree.internalCount) {
          nextNeighbor = Some(col, tree.bits.count(tree.internalCount, pos - 1))
        } else {
          // Update col of next child node
          col = col * tree.k + childIndex % tree.k
          currSize /= tree.k
          height += 1

          val nextLevelOffset = levelOffsets(height)
          val blockStart = nextLevelOffset + tree.bits.count(levelOffset, pos - 1) * k2
          val localLine = line % (currSize * tree.k)
          val subBlockStart = blockStart + (localLine / currSize) * tree.k

          cursors(height) = K2TreeDirectNeighborCursor(subBlockStart)
        }
      }
    }

    nextNeighbor
  }
}
