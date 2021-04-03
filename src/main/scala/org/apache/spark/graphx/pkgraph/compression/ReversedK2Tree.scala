package org.apache.spark.graphx.pkgraph.compression

class ReversedK2Tree(tree: K2Tree) extends K2Tree(tree.k, tree.size, tree.bits, tree.internalCount, tree.leavesCount) {
  override def iterator: K2TreeIterator = new K2TreeIterator(this, true)
}
