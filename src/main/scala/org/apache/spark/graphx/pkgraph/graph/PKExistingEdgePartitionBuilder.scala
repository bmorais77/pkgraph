package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.graphx.{VertexId, VertexSet}
import org.apache.spark.graphx.pkgraph.compression.K2TreeBuilder
import org.apache.spark.graphx.pkgraph.util.collection.PKBitSet
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.util.collection.PrimitiveVector

import scala.reflect.ClassTag

private[graph] class PKExistingEdgePartitionBuilder[V: ClassTag, @specialized(Long, Int, Double) E: ClassTag](
    size: Int,
    vertexAttrs: GraphXPrimitiveKeyOpenHashMap[VertexId, V],
    builder: K2TreeBuilder,
    srcOffset: Long,
    dstOffset: Long,
    srcVertices: PKBitSet,
    dstVertices: PKBitSet,
    activeSet: Option[VertexSet]
) {
  private val edges = new PrimitiveVector[PKEdge[E]](size)

  def addEdge(src: VertexId, dst: VertexId, attr: E): Unit = {
    val line = (src - srcOffset).toInt
    val col = (dst - dstOffset).toInt
    val index = builder.addEdge(line, col)

    srcVertices.set(line)
    dstVertices.set(col)
    edges += PKEdge(index, line, col, attr)
  }

  def removeEdge(src: VertexId, dst: VertexId): Unit = {
    val line = (src - srcOffset).toInt
    val col = (dst - dstOffset).toInt
    builder.removeEdge(line, col)

    srcVertices.unset(line)
    dstVertices.unset(col)
  }

  def build: PKEdgePartition[V, E] = {
    val edgeAttrs = edges
      .toArray
      .filter { e => srcVertices.get(e.line) && dstVertices.get(e.col) }
      .sortWith((a, b) => a.index < b.index)
      .map(_.attr)

    new PKEdgePartition[V, E](
      vertexAttrs,
      edgeAttrs,
      builder.build,
      srcOffset,
      dstOffset,
      srcVertices,
      dstVertices,
      activeSet
    )
  }
}

object PKExistingEdgePartitionBuilder {

  /**
    * Partition builder from a tree builder.
    *
    * @param partition       Existing partition
    * @param treeBuilder     Non-empty tree builder
    * @param srcOffset       New source offset
    * @param dstOffset       New destination offset
    * @tparam V              Type of vertex attributes
    * @tparam E              Type of edge attributes
    * @return existing partition builder
    */
  def apply[V: ClassTag, E: ClassTag](
      partition: PKEdgePartition[V, _],
      treeBuilder: K2TreeBuilder,
      srcOffset: Long = 0,
      dstOffset: Long = 0
  ): PKExistingEdgePartitionBuilder[V, E] = {
    new PKExistingEdgePartitionBuilder[V, E](
      partition.size,
      partition.vertexAttrs,
      treeBuilder,
      srcOffset,
      dstOffset,
      new PKBitSet(treeBuilder.size),
      new PKBitSet(treeBuilder.size),
      partition.activeSet
    )
  }
}
