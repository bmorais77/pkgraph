package org.apache.spark.graphx.pkgraph.compression

class K2TreeIterator(tree: K2Tree) extends Iterator[(Int, Int)] {
  private val k2 = tree.k * tree.k
  private val levelOffsets = buildLevelOffsets(tree)

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
        line = line / tree.k * tree.k + childIndex / tree.k
        col = col / tree.k * tree.k + childIndex % tree.k

        // Leaf node
        if (pos >= tree.internalCount) {
          nextEdge = Some((line, col))
        } else {
          line = line * tree.k + childIndex / tree.k
          col = col * tree.k + childIndex % tree.k

          height += 1
          nextBlock = true
        }
      }
    }

    nextEdge
  }

  /**
    * Builds an array containing the offset in the tree's bitset of each level.
    *
    * @param tree    Tree to build level offsets for
    * @return offsets for each level
    */
  private def buildLevelOffsets(tree: K2Tree): Array[Int] = {
    if (tree.isEmpty) {
      return Array.empty
    }

    val height = tree.height
    val offsets = new Array[Int](height)
    offsets(0) = 0 // First level is always at beginning of the bitset

    if (height > 1) {
      offsets(1) = k2 // Second level is always as an offset of K² bits
    }

    var start = 0
    var end = k2
    for (level <- 2 until height) {
      offsets(level) = offsets(level - 1) + tree.bits.count(start, end - 1) * k2
      start = end
      end = offsets(level)
    }

    offsets
  }
}
