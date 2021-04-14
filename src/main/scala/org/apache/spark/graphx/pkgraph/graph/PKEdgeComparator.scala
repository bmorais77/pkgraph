package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.graphx.pkgraph.compression.K2TreeIndex

import scala.reflect.ClassTag

private[graph] class PKEdgeComparator[E1: ClassTag, E2: ClassTag](
    part1: PKEdgePartition[_, E1],
    part2: PKEdgePartition[_, E2]
) {
  def compare(e1: PKEdge[E1], e2: PKEdge[E2]): Int = {
    // Special case where the K values of the trees are the same
    if(part1.tree.k == part2.tree.k) {
      return e1.index - e2.index
    }

    // Find where the global edge `e2` is locally in `part1`
    val localLine = (e2.line - part1.srcOffset).toInt
    val localCol = (e2.col - part1.dstOffset).toInt
    val e2Index = K2TreeIndex.fromEdge(part1.tree.k, part1.tree.height, localLine, localCol)

    e1.index - e2Index
  }
}
