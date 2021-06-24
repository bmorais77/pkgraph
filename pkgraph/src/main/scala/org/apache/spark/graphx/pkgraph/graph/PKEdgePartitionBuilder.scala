package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.graphx.pkgraph.compression.K2TreeBuilder
import org.apache.spark.graphx.pkgraph.util.collection.PKBitSet
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.util.collection.PrimitiveVector

import scala.collection.mutable
import scala.reflect.ClassTag

private[pkgraph] class PKEdgePartitionBuilder[V: ClassTag, E: ClassTag] private (k: Int, size: Int) {
  private val edges = new PrimitiveVector[Edge[E]](size)
  private val vertices = new mutable.HashSet[VertexId]

  private var startX: Long = Long.MaxValue
  private var startY: Long = Long.MaxValue

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
    vertices.add(src)
    vertices.add(dst)
  }

  def build: PKEdgePartition[V, E] = {
    val k2 = k * k

    // Align origin to nearest multiple of K
    val srcOffset = if (startX % k2 == 0) startX else startX / k2
    val dstOffset = if (startY % k2 == 0) startY else startY / k2

    val edgeArray = edges.trim().array
    val treeBuilder = if (edgeArray.isEmpty) {
      K2TreeBuilder(k, 0)
    } else {
      K2TreeBuilder(k, math.max(endX - srcOffset + 1, endY - dstOffset + 1).toInt)
    }

    val srcVertices = new PKBitSet(treeBuilder.size)
    val dstVertices = new PKBitSet(treeBuilder.size)
    val sortedEdges = mutable.TreeSet[(Int, Edge[E])]()((a, b) => a._1 - b._1)
    for (edge <- edgeArray) {
      val localSrcId = (edge.srcId - srcOffset).toInt
      val localDstId = (edge.dstId - dstOffset).toInt
      val index = treeBuilder.addEdge(localSrcId, localDstId)

      srcVertices.set(localSrcId)
      dstVertices.set(localDstId)

      // Our solution does not support multi-graphs, so we ignore repeated edges
      sortedEdges.add((index, edge))
    }

    // Traverse sorted edges to construct global2local map
    val global2local = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]
    var currLocalId = -1
    for ((_, edge) <- sortedEdges) {
      global2local.changeValue(edge.srcId, { currLocalId += 1; currLocalId }, identity)
      global2local.changeValue(edge.dstId, { currLocalId += 1; currLocalId }, identity)
    }

    val edgeAttrs = sortedEdges.toArray.map(_._2.attr)
    val vertexAttrs = new Array[V](currLocalId + 1)
    new PKEdgePartition[V, E](
      vertexAttrs,
      global2local,
      edgeAttrs,
      treeBuilder.build,
      srcOffset,
      dstOffset,
      srcVertices,
      dstVertices,
      None
    )
  }
}

object PKEdgePartitionBuilder {
  def apply[V: ClassTag, E: ClassTag](k: Int, size: Int = 64): PKEdgePartitionBuilder[V, E] = {
    new PKEdgePartitionBuilder[V, E](k, size)
  }
}
