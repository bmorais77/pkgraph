package pt.tecnico.ulisboa.meic.compression

class ReversedK2Tree(tree: K2Tree) extends K2Tree(tree.k, tree.size, tree.bits, tree.internalCount, tree.leavesCount) {
  override protected def iterateEdges(f: (Int, Int) => Unit, currSize: Int, line: Int, col: Int, pos: Int): Unit = {
    if (pos >= internalCount) { // Is leaf node
      if (bits.get(pos)) {
        f(line, col)
        return
      }
    } else {
      if (pos == -1 || bits.get(pos)) {
        val y = rank(bits, pos) * k * k
        val newSize = currSize / k

        for (i <- k * k - 1 to 0 by -1) {
          iterateEdges(f, newSize, line * k + i / k, col * k + i % k, y + i)
        }
      }
    }
  }
}
