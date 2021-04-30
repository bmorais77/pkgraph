package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.graphx.pkgraph.compression.K2TreeBuilder
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.util.collection.{BitSet, PrimitiveVector}

import scala.collection.mutable
import scala.reflect.ClassTag

private[graph] class PKEdgePartitionBuilder[V: ClassTag, E: ClassTag] private (k: Int) {
  private val edges = new PrimitiveVector[Edge[E]]
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

    val attrs = mutable.TreeSet[(Int, E)]()((a, b) => a._1 - b._1)
    for (edge <- edgeArray) {
      val localSrcId = (edge.srcId - srcOffset).toInt
      val localDstId = (edge.dstId - dstOffset).toInt
      val index = treeBuilder.addEdge(localSrcId, localDstId)

      // Our solution does not support multi-graphs, so we ignore repeated edges
      attrs.add((index, edge.attr))
    }

    var pos = 0
    val indices = new GraphXPrimitiveKeyOpenHashMap[Int, Int]
    for ((index, _) <- attrs) {
      indices(index) = pos
      pos += 1
    }

    val edgeAttrs = attrs.toArray.map(_._2)
    new PKEdgePartition[V, E](
      new GraphXPrimitiveKeyOpenHashMap[VertexId, V](vertices.size),
      new EdgeAttributesMap[E](indices, edgeAttrs),
      treeBuilder.build,
      srcOffset,
      dstOffset,
      None
    )
  }
}

object PKEdgePartitionBuilder {
  def apply[V: ClassTag, E: ClassTag](k: Int): PKEdgePartitionBuilder[V, E] = {
    new PKEdgePartitionBuilder[V, E](k)
  }
}
