package org.apache.spark.graphx.pkgraph.compression

class K2TreeIterator(tree: K2Tree) extends Iterator[(Int, Int)] {
  private val k2 = tree.k * tree.k

  // Number of quadrants per line/column one level above the leaves
  private val levelSize = if(tree.size == 0) 0 else tree.size / tree.k

  private var leafBlockIndex = 0

  // Position inside the tree bitset
  private var leafPos = -1

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

    while (nextEdge.isEmpty && leafBlockIndex < tree.leafIndices.length) {
      leafPos = tree.bits.nextSetBit(leafPos + 1)

      // Reached end of tree or end of leaf block
      if (leafPos == -1 || leafPos >= k2 * (leafBlockIndex + 1)) {
        leafBlockIndex += 1
      }

      // Reached end of leaf blocks
      if(leafBlockIndex >= tree.leafIndices.length) {
        return None
      }

      // Safe to cast to Int since the matrix size is at most 2^31
      val lineOffset = (tree.leafIndices(leafBlockIndex) / levelSize).toInt
      val colOffset = (tree.leafIndices(leafBlockIndex) % levelSize).toInt

      val childIndex = leafPos % k2
      val line = lineOffset * tree.k + (childIndex / tree.k)
      val col = colOffset * tree.k + (childIndex % tree.k)
      nextEdge = Some((line, col))
    }

    nextEdge
  }
}
