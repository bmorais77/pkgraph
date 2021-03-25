package pt.tecnico.ulisboa.meic.graph.impl

import org.apache.spark.graphx.VertexId

import scala.reflect.ClassTag

/** Stores vertex attributes to ship to an edge partition. */
private[graph] class PKVertexAttributeBlock[VD: ClassTag](val vids: Array[VertexId], val attrs: Array[VD])
    extends Serializable {
  def iterator: Iterator[(VertexId, VD)] = vids.indices.iterator.map { i => (vids(i), attrs(i)) }
}
