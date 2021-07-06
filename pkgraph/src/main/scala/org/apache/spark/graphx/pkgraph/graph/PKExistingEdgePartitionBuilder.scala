package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.graphx.{VertexId, VertexSet}
import org.apache.spark.graphx.pkgraph.compression.K2TreeBuilder
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.util.collection.PrimitiveVector

import scala.reflect.ClassTag

/**
  * Edge partition builder for existing partitions.
  *
  * @param k                 Value of the K²-Tree
  * @param size              Size of the matrix to forward to builder
  * @param vertexAttrs       Existing vertex attributes
  * @param global2local      Existing global2local vertex mapping
  * @param srcOffset         Offset for source vertices
  * @param dstOffset         Offset for destination vertices
  * @param activeSet         Existing active set of vertices
  * @tparam V                Type of vertex attributes
  * @tparam E                Type of edge attributes
  */
private[pkgraph] class PKExistingEdgePartitionBuilder[V: ClassTag, @specialized(Long, Int, Double) E: ClassTag](
    k: Int,
    size: Int,
    vertexAttrs: Array[V],
    global2local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int],
    srcOffset: Long,
    dstOffset: Long,
    activeSet: Option[VertexSet]
) {
  private val builder = K2TreeBuilder(k, size)
  private val edgeAttrs = new PrimitiveVector[E](64)

  private var lastLine = -1
  private var lastCol = -1

  def addEdge(src: VertexId, dst: VertexId, attr: E): Unit = {
    val line = (src - srcOffset).toInt
    val col = (dst - dstOffset).toInt

    // Our solution does not support multi-graphs, so we ignore repeated edges
    if(lastLine != line || lastCol != col) {
      lastLine = line
      lastCol = col

      builder.addEdge(line, col)
      edgeAttrs += attr
    }
  }

  def build(): PKEdgePartition[V, E] = {
    new PKEdgePartition[V, E](
      vertexAttrs,
      global2local,
      edgeAttrs.toArray,
      builder.build(),
      srcOffset,
      dstOffset,
      activeSet
    )
  }
}

object PKExistingEdgePartitionBuilder {
  /**
    * Creates a new builder from an existing partition but with no edges.
    *
    * @param partition        Partition to build from
    * @tparam V               Type of vertex attributes
    * @tparam E               Type of edge attributes
    * @return existing partition builder
    */
  def apply[V: ClassTag, E: ClassTag](partition: PKEdgePartition[V, _]): PKExistingEdgePartitionBuilder[V, E] = {
    new PKExistingEdgePartitionBuilder[V, E](
      partition.tree.k,
      partition.tree.size,
      partition.vertexAttrs,
      partition.global2local,
      partition.srcOffset,
      partition.dstOffset,
      partition.activeSet
    )
  }
}
