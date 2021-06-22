package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.pkgraph.compression.K2TreeIndex

import scala.reflect.ClassTag

private[graph] class PKEdgeComparator[E1: ClassTag, E2: ClassTag](
    part1: PKEdgePartition[_, E1],
    part2: PKEdgePartition[_, E2]
) {
  private val tree1Height = part1.tree.height
  private val height = math.max(tree1Height, part2.tree.height)
  private val srcOffset = math.min(part1.srcOffset, part2.srcOffset)
  private val dstOffset = math.min(part1.dstOffset, part2.dstOffset)

  def compare(e1: Edge[E1], e2: Edge[E2]): Int = {
    val line1 = (e1.srcId - srcOffset).toInt
    val col1 = (e1.dstId - dstOffset).toInt
    val line2 = (e2.srcId - srcOffset).toInt
    val col2 = (e2.dstId - dstOffset).toInt
    val idx1 = K2TreeIndex.fromEdge(part1.tree.k, height, line1, col1)
    val idx2 = K2TreeIndex.fromEdge(part2.tree.k, height, line2, col2)
    idx1 - idx2
  }
}
