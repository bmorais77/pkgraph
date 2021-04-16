package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.graphx.pkgraph.compression.{K2TreeBuilder, K2TreeIndex}
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.util.collection.BitSet

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

private[graph] class PKEdgePartitionBuilder[V: ClassTag, E: ClassTag] private (
    k: Int,
    vertexAttrs: GraphXPrimitiveKeyOpenHashMap[VertexId, V]
) {
  // Inserts the edges in a ordered fashion
  private val edges = new ArrayBuffer[Edge[E]]

  private var startX: Long = 0
  private var startY: Long = 0

  // Start at -1 so that if no edges are added the builder will build a K²-Tree with size 0
  private var endX: Long = 0
  private var endY: Long = 0

  /**
    * Adds the edge with the given vertices and attribute to this builder.
    *
    * @param src Source vertex identifier
    * @param dst Destination vertex identifier
    * @param attr Edge attribute
    */
  def add(src: VertexId, dst: VertexId, attr: E): Unit = {
    edges += Edge(src, dst, attr)
    startX = math.min(startX, src)
    startY = math.min(startY, dst)
    endX = math.max(endX, src)
    endY = math.max(endY, dst)
  }

  def build: PKEdgePartition[V, E] = {
    val treeBuilder = K2TreeBuilder(k, math.max(endX - startX + 1, endY - startY + 1).toInt)
    var pos = 0
    val unsortedAttrs = new Array[(Int, E)](edges.size)
    val edgeIndices = new BitSet(treeBuilder.size * treeBuilder.size)
    val srcIndex = new BitSet(treeBuilder.size)
    val dstIndex = new BitSet(treeBuilder.size)

    for (edge <- edges) {
      val localSrcId = (edge.srcId - startX).toInt
      val localDstId = (edge.dstId - startY).toInt

      srcIndex.set(localSrcId)
      dstIndex.set(localDstId)

      treeBuilder.addEdge(localSrcId, localDstId)
      val index = K2TreeIndex.fromEdge(treeBuilder.k, treeBuilder.height, localSrcId, localDstId)

      // Our solution does not support multi-graphs, so we ignore repeated edges
      unsortedAttrs(pos) = (index, edge.attr)
      edgeIndices.set(index)
      pos += 1
    }

    val edgeAttrs = unsortedAttrs.sortWith((a, b) => a._1 < b._1).map(_._2)
    val activeSet = new BitSet(0)
    new PKEdgePartition[V, E](
      vertexAttrs,
      edgeAttrs,
      edgeIndices,
      treeBuilder.build,
      startX,
      startY,
      activeSet,
      srcIndex,
      dstIndex
    )
  }
}

object PKEdgePartitionBuilder {
  def apply[V: ClassTag, E: ClassTag](k: Int): PKEdgePartitionBuilder[V, E] = {
    new PKEdgePartitionBuilder[V, E](k, new GraphXPrimitiveKeyOpenHashMap[VertexId, V])
  }

  def existing[V: ClassTag, E: ClassTag](
      k: Int,
      vertexAttrs: GraphXPrimitiveKeyOpenHashMap[VertexId, V]
  ): PKEdgePartitionBuilder[V, E] = {
    new PKEdgePartitionBuilder[V, E](k, vertexAttrs)
  }
}
