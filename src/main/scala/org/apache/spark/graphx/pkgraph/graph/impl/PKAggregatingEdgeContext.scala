package org.apache.spark.graphx.pkgraph.graph.impl

import org.apache.spark.graphx.pkgraph.util.collection.PrimitiveHashMap
import org.apache.spark.graphx.{EdgeContext, VertexId}

import scala.reflect.ClassTag

private[impl] class PKAggregatingEdgeContext[V, E, A] private (
    mergeMsg: (A, A) => A,
    aggregates: PrimitiveHashMap[VertexId, A]
) extends EdgeContext[V, E, A] {

  private[this] var _srcId: VertexId = _
  private[this] var _dstId: VertexId = _
  private[this] var _srcAttr: V = _
  private[this] var _dstAttr: V = _
  private[this] var _attr: E = _

  def iterator: Iterator[(VertexId, A)] = aggregates.iterator

  def set(srcId: VertexId, dstId: VertexId, srcAttr: V, dstAttr: V, attr: E): Unit = {
    _srcId = srcId
    _dstId = dstId
    _srcAttr = srcAttr
    _dstAttr = dstAttr
    _attr = attr
  }

  def setSrcOnly(srcId: VertexId, srcAttr: V): Unit = {
    _srcId = srcId
    _srcAttr = srcAttr
  }

  def setDest(dstId: VertexId, dstAttr: V, attr: E): Unit = {
    _dstId = dstId
    _dstAttr = dstAttr
    _attr = attr
  }

  override def srcId: VertexId = _srcId
  override def dstId: VertexId = _dstId
  override def srcAttr: V = _srcAttr
  override def dstAttr: V = _dstAttr
  override def attr: E = _attr

  override def sendToSrc(msg: A): Unit = {
    send(_srcId, msg)
  }
  override def sendToDst(msg: A): Unit = {
    send(_dstId, msg)
  }

  @inline private def send(id: VertexId, msg: A): Unit = {
    aggregates.changeValue(id, msg, mergeMsg(_, msg))
  }
}

object PKAggregatingEdgeContext {
  def apply[V, E, A: ClassTag](mergeMsg: (A, A) => A): PKAggregatingEdgeContext[V, E, A] =
    new PKAggregatingEdgeContext(mergeMsg, new PrimitiveHashMap[VertexId, A])
}
