package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.pkgraph.compression.K2TreeBuilder
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.util.collection.BitSet

import scala.collection.mutable
import scala.reflect.ClassTag

private[graph] class PKExistingEdgePartitionBuilder[V: ClassTag, @specialized(Long, Int, Double) E: ClassTag](
    vertexAttrs: GraphXPrimitiveKeyOpenHashMap[VertexId, V],
    builder: K2TreeBuilder,
    srcOffset: Long,
    dstOffset: Long,
    activeSet: BitSet,
    srcIndex: BitSet,
    dstIndex: BitSet
) {
  private val edges = mutable.HashMap[Int, E]()
  private val edgeIndices = new BitSet(builder.size * builder.size)

  def addEdge(src: VertexId, dst: VertexId, attr: E): Unit = {
    val line = (src - srcOffset).toInt
    val col = (dst - dstOffset).toInt
    val index = builder.addEdge(line, col)

    edges(index) = attr
    srcIndex.set(line)
    dstIndex.set(col)
    edgeIndices.set(index)
  }

  def removeEdge(src: VertexId, dst: VertexId): Unit = {
    val line = (src - srcOffset).toInt
    val col = (dst - dstOffset).toInt
    val index = builder.removeEdge(line, col)

    edges.remove(index)
    srcIndex.unset(line)
    dstIndex.unset(col)
  }

  def build: PKEdgePartition[V, E] = {
    val edgeArray = edges.toArray.sortWith((a, b) => a._1 < b._1)
    val attrValues = edgeArray.map(_._2)
    val edgeAttrs = new EdgeAttributesMap[E](edgeIndices, attrValues)
    new PKEdgePartition[V, E](
      vertexAttrs,
      edgeAttrs,
      builder.build,
      srcOffset,
      dstOffset,
      activeSet,
      srcIndex,
      dstIndex
    )
  }
}

object PKExistingEdgePartitionBuilder {
  def apply[V: ClassTag, E: ClassTag](
      partition: PKEdgePartition[V, _],
      treeBuilder: K2TreeBuilder
  ): PKExistingEdgePartitionBuilder[V, E] = {
    new PKExistingEdgePartitionBuilder[V, E](
      partition.vertexAttrs,
      treeBuilder,
      partition.srcOffset,
      partition.dstOffset,
      partition.activeSet,
      partition.srcIndex,
      partition.dstIndex
    )
  }

  def apply[V: ClassTag, E: ClassTag](
      partition: PKEdgePartition[V, _],
      treeBuilder: K2TreeBuilder,
      srcOffset: Long,
      dstOffset: Long
  ): PKExistingEdgePartitionBuilder[V, E] = {
    new PKExistingEdgePartitionBuilder[V, E](
      partition.vertexAttrs,
      treeBuilder,
      srcOffset,
      dstOffset,
      partition.activeSet,
      partition.srcIndex,
      partition.dstIndex
    )
  }
}
