package org.apache.spark.graphx.pkgraph.graph.impl

import org.apache.spark.graphx.pkgraph.compression.K2TreeBuilder
import org.apache.spark.graphx.pkgraph.util.collection.PrimitiveHashMap
import org.apache.spark.graphx.{Edge, VertexId}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

private[impl] class PKEdgePartitionBuilder[V: ClassTag, E: ClassTag] private(
    k: Int,
    vertexAttrs: PrimitiveHashMap[VertexId, V]
) {
  // Inserts the edges in a ordered fashion
  private val edges = new ArrayBuffer[Edge[E]]

  private var startX: Long = 0
  private var startY: Long = 0
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
    val treeBuilder = K2TreeBuilder(k, math.max(endX - startX, endY - startY).toInt)
    val data = new mutable.TreeSet[(Int, E)]()((x, y) => x._1 - y._1)

    var i = 0
    for (edge <- edges) {
      val localSrcId = (edge.srcId - startX).toInt
      val localDstId = (edge.dstId - startY).toInt

      val index = treeBuilder.addEdge(localSrcId, localDstId)
      if (index != -1) {
        // Our solution does not support multi-graphs, so we ignore repeated edges
        // TODO: The user could define a merge function and we could merge the attributes,
        // TODO: but this function would need to be defined in the Graph and shipped to the
        // TODO: remote processors
        data.add((index, edge.attr))
        i += 1
      }
    }

    val attrs = data.toArray.map(_._2)
    new PKEdgePartition[V, E](vertexAttrs, attrs, treeBuilder.build, startX, startY)
  }
}

object PKEdgePartitionBuilder {
  def apply[V: ClassTag, E: ClassTag](k: Int): PKEdgePartitionBuilder[V, E] = {
    new PKEdgePartitionBuilder[V, E](k, new PrimitiveHashMap[VertexId, V])
  }

  def existing[V: ClassTag, E: ClassTag](k: Int, vertexAttrs: PrimitiveHashMap[VertexId, V]): PKEdgePartitionBuilder[V, E] = {
    new PKEdgePartitionBuilder[V, E](k, vertexAttrs)
  }
}
