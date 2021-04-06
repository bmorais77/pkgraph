package org.apache.spark.graphx.pkgraph.graph.impl

import org.apache.spark.graphx.VertexId

import scala.reflect.ClassTag

/** Stores vertex attributes to ship to an edge partition. */
private[impl] class PKVertexAttributeBlock[VD: ClassTag](val vids: Array[VertexId], val attrs: Array[VD])
    extends Serializable {
  def iterator: Iterator[(VertexId, VD)] = vids.indices.iterator.map { i => (vids(i), attrs(i)) }
}
