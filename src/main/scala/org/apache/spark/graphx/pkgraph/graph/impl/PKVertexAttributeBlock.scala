package org.apache.spark.graphx.pkgraph.graph.impl

import org.apache.spark.graphx.VertexId

import scala.reflect.ClassTag

/** Stores vertex attributes to ship to an edge partition. */
private[impl] class PKVertexAttributeBlock[V: ClassTag](val vids: Array[VertexId], val attrs: Array[V])
    extends Serializable {
  def iterator: Iterator[(VertexId, V)] = vids.indices.iterator.map { i => (vids(i), attrs(i)) }
}
