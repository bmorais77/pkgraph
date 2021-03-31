package org.apache.spark.graphx.pkgraph.graph.impl

import org.apache.spark.graphx.pkgraph.compression.K2TreeBuilder
import org.apache.spark.graphx.{Edge, VertexId}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

private[graph] class PKEdgePartitionBuilder[V: ClassTag, E: ClassTag](k: Int) {
  private val edges = new ArrayBuffer[Edge[E]](64)

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
    val data = new Array[E](edges.length)
    val treeBuilder = K2TreeBuilder(k, math.max(endX - startX, endY - startY).toInt)

    var i = 0
    while (i < edges.length) {
      val srcId = edges(i).srcId
      val dstId = edges(i).dstId
      val localSrcId = (srcId - startX).toInt
      val localDstId = (dstId - startY).toInt

      treeBuilder.addEdges(Seq((localSrcId, localDstId)))
      data(i) = edges(i).attr

      i += 1
    }

    new PKEdgePartition[V, E](Map.empty[VertexId, V], data, treeBuilder.build(), startX, startY)
  }
}
