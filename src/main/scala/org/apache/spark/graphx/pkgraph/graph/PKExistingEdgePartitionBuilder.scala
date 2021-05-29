package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.graphx.{VertexId, VertexSet}
import org.apache.spark.graphx.pkgraph.compression.K2TreeBuilder
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap

import scala.collection.mutable
import scala.reflect.ClassTag

private[graph] class PKExistingEdgePartitionBuilder[V: ClassTag, @specialized(Long, Int, Double) E: ClassTag](
    vertexAttrs: GraphXPrimitiveKeyOpenHashMap[VertexId, V],
    builder: K2TreeBuilder,
    indexOffset: Int,
    existingEdges: Array[E],
    srcOffset: Long,
    dstOffset: Long,
    activeSet: Option[VertexSet]
) {
  private val edges = mutable.HashMap[Int, E]()

  private var idx = 0
  for (attr <- existingEdges) {
    edges(indexOffset + idx) = attr
    idx += 1
  }

  def addEdge(src: VertexId, dst: VertexId, attr: E): Unit = {
    val line = (src - srcOffset).toInt
    val col = (dst - dstOffset).toInt
    val index = builder.addEdge(line, col)

    edges(index) = attr
  }

  def removeEdge(src: VertexId, dst: VertexId): Unit = {
    val line = (src - srcOffset).toInt
    val col = (dst - dstOffset).toInt
    val index = builder.removeEdge(line, col)

    edges.remove(index)
  }

  def build: PKEdgePartition[V, E] = {
    val edgeAttrs = edges.toArray.sortWith((a, b) => a._1 < b._1).map(_._2)
    new PKEdgePartition[V, E](
      vertexAttrs,
      edgeAttrs,
      builder.build,
      srcOffset,
      dstOffset,
      activeSet
    )
  }
}

object PKExistingEdgePartitionBuilder {

  /**
    * Builder from an empty tree builder with no initial edge attributes.
    *
    * @param partition     Existing partition
    * @param treeBuilder   Existing tree builder (should be empty)
    * @tparam V            Type of vertex attributes
    * @tparam E            Type of edge attributes
    * @return existing partition builder
    */
  def apply[V: ClassTag, E: ClassTag](
      partition: PKEdgePartition[V, _],
      treeBuilder: K2TreeBuilder
  ): PKExistingEdgePartitionBuilder[V, E] = {
    new PKExistingEdgePartitionBuilder[V, E](
      partition.vertexAttrs,
      treeBuilder,
      0,
      Array.empty,
      partition.srcOffset,
      partition.dstOffset,
      partition.activeSet
    )
  }

  /**
    * Builder from a non-empty tree builder with existing edge attributes.
    *
    * @param partition       Existing partition
    * @param treeBuilder     Non-empty tree builder
    * @param existingEdges   Existing edge attributes
    * @param srcOffset       New source offset
    * @param dstOffset       New destination offset
    * @tparam V              Type of vertex attributes
    * @tparam E              Type of edge attributes
    * @return existing partition builder
    */
  def apply[V: ClassTag, E: ClassTag](
      partition: PKEdgePartition[V, _],
      treeBuilder: K2TreeBuilder,
      existingEdges: Array[E],
      srcOffset: Long,
      dstOffset: Long
  ): PKExistingEdgePartitionBuilder[V, E] = {
    // Check if new virtual origin is behind previous origin
    // If so apply an index offset to all edges according to how many levels changed in the K²-Tree
    val indexOffset = if (srcOffset < partition.srcOffset || dstOffset < partition.dstOffset) {
      val k2 = treeBuilder.k * treeBuilder.k
      var offset = 0
      var heightChange = treeBuilder.height - partition.tree.height
      var currSize = treeBuilder.size
      while (heightChange > 0) {
        // The existing edges will be placed in the last quadrant at every level
        val localIndex = (treeBuilder.k - 1) * treeBuilder.k + (treeBuilder.k - 1)
        offset += localIndex * currSize
        currSize /= k2
        heightChange -= 1
      }
      offset
    } else {
      0
    }

    new PKExistingEdgePartitionBuilder[V, E](
      partition.vertexAttrs,
      treeBuilder,
      indexOffset,
      existingEdges,
      srcOffset,
      dstOffset,
      partition.activeSet
    )
  }
}
