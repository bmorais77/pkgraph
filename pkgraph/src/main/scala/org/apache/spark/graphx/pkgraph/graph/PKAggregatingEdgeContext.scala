package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.graphx.pkgraph.util.collection.Bitset
import org.apache.spark.graphx.{EdgeContext, VertexId}

import scala.reflect.ClassTag

private[pkgraph] class PKAggregatingEdgeContext[V, E, A](vertexCount: Int, mergeMsg: (A, A) => A)
    extends EdgeContext[V, E, A] {

  case class EdgeMessage(vid: VertexId, var aggregate: A)

  private val aggregates = new Array[EdgeMessage](vertexCount)
  private val indices = new Bitset(vertexCount)

  private[this] var _srcId: VertexId = _
  private[this] var _dstId: VertexId = _
  private[this] var _localSrcId: Int = _
  private[this] var _localDstId: Int = _
  private[this] var _srcAttr: V = _
  private[this] var _dstAttr: V = _
  private[this] var _attr: E = _

  def iterator: Iterator[(VertexId, A)] = indices.iterator.map(i => (aggregates(i).vid, aggregates(i).aggregate))

  def set(srcId: VertexId, dstId: VertexId, localSrcId: Int, localDstId: Int, srcAttr: V, dstAttr: V, attr: E): Unit = {
    _srcId = srcId
    _dstId = dstId
    _localSrcId = localSrcId
    _localDstId = localDstId
    _srcAttr = srcAttr
    _dstAttr = dstAttr
    _attr = attr
  }

  override def srcId: VertexId = _srcId
  override def dstId: VertexId = _dstId
  override def srcAttr: V = _srcAttr
  override def dstAttr: V = _dstAttr
  override def attr: E = _attr

  override def sendToSrc(msg: A): Unit = {
    send(_srcId, _localSrcId, msg)
  }
  override def sendToDst(msg: A): Unit = {
    send(_dstId, _localDstId, msg)
  }

  @inline private def send(vid: VertexId, localId: Int, msg: A): Unit = {
    if (indices.get(localId)) {
      // Array position already contains an attribute so merge attributes
      aggregates(localId).aggregate = mergeMsg(aggregates(localId).aggregate, msg)
    } else {
      // Array does not yet have the attribute for the given vertex
      aggregates(localId) = EdgeMessage(vid, msg)
      indices.set(localId)
    }
  }
}
