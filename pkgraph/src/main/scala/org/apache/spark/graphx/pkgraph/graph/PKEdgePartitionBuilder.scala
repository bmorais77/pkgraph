package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.graphx.pkgraph.compression.{K2TreeBuilder, K2TreeIndex}
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.util.collection.PrimitiveVector

import scala.reflect.ClassTag

private[pkgraph] class PKEdgePartitionBuilder[V: ClassTag, E: ClassTag] private (k: Int, initialCapacity: Int) {
  private val edges = new PrimitiveVector[Edge[E]](initialCapacity)

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
  }

  def build(): PKEdgePartition[V, E] = {
    // Special case of empty partition
    if (edges.length == 0) {
      return PKEdgePartition.empty
    }

    val srcOffset = startX
    val dstOffset = startY
    val matrixSize = math.max(endX - srcOffset + 1, endY - dstOffset + 1).toInt

    val builder = K2TreeBuilder(k, matrixSize)
    val sortedEdgesWithIndex = edges
      .trim()
      .array
      .map { edge =>
        val line = (edge.srcId - srcOffset).toInt
        val col = (edge.dstId - dstOffset).toInt
        (K2TreeIndex.fromEdge(k, builder.height, line, col), edge)
      }
      .sortBy(e => e._1)

    // Count the number of unique edges and map the edges to remove the index
    val sortedEdges = new Array[Edge[E]](sortedEdgesWithIndex.length)
    var edgeCount = 0
    var lastLine = -1
    var lastCol = -1

    for ((_, edge) <- sortedEdgesWithIndex) {
      val line = (edge.srcId - srcOffset).toInt
      val col = (edge.dstId - dstOffset).toInt

      // Our solution does not support multi-graphs, so we ignore repeated edges
      if (lastLine != line || lastCol != col) {
        builder.addEdge(line, col)
        lastLine = line
        lastCol = col

        sortedEdges(edgeCount) = edge
        edgeCount += 1
      }
    }

    val tree = builder.build()

    // Traverse sorted edges to construct global2local map and the final edge attribute array
    val edgeAttrs = new Array[E](edgeCount)
    val global2local = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]
    var currLocalId = -1

    for (i <- 0 until edgeCount) {
      val edge = sortedEdges(i)
      edgeAttrs(i) = edge.attr

      global2local.changeValue(edge.srcId, { currLocalId += 1; currLocalId }, identity)
      global2local.changeValue(edge.dstId, { currLocalId += 1; currLocalId }, identity)
    }

    val vertexAttrs = new Array[V](currLocalId + 1)
    new PKEdgePartition[V, E](vertexAttrs, global2local, edgeAttrs, tree, srcOffset, dstOffset, None)
  }
}

object PKEdgePartitionBuilder {

  /**
    * Creates a new partition builder.
    *
    * @param k                    Value of K²-Tree
    * @param initialCapacity      Initial capacity of edges
    * @tparam V                   Type of vertex attributes
    * @tparam E                   Type of edge attributes
    * @return new empty partition builder
    */
  def apply[V: ClassTag, E: ClassTag](k: Int, initialCapacity: Int = 64): PKEdgePartitionBuilder[V, E] = {
    new PKEdgePartitionBuilder[V, E](k, initialCapacity)
  }
}
