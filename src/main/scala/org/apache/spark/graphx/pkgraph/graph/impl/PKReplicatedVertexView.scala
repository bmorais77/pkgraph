package org.apache.spark.graphx.pkgraph.graph.impl

import org.apache.spark.graphx.impl.VertexAttributeBlock
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class PKReplicatedVertexView[V: ClassTag, E: ClassTag](
    var edges: PKEdgeRDDImpl[V, E],
    var hasSrcId: Boolean = false,
    var hasDstId: Boolean = false
) {

  /**
    * Return a new `ReplicatedVertexView` with the specified `EdgeRDD`, which must have the same
    * shipping level.
    */
  def withEdges[V2: ClassTag, E2: ClassTag](edgesRDD: PKEdgeRDDImpl[V2, E2]): PKReplicatedVertexView[V2, E2] = {
    new PKReplicatedVertexView(edgesRDD, hasSrcId, hasDstId)
  }

  /**
    * Return a new `ReplicatedVertexView` where edges are reversed and shipping levels are swapped to
    * match.
    */
  def reverse(): PKReplicatedVertexView[V, E] = {
    val newEdges = edges.mapEdgePartitions[V, E]((_, part) => part.reverse)
    new PKReplicatedVertexView(newEdges, hasDstId, hasSrcId)
  }

  /**
    * Upgrade the shipping level in-place to the specified levels by shipping vertex attributes from
    * `vertices`. This operation modifies the `ReplicatedVertexView`, and callers can access `edges`
    * afterwards to obtain the upgraded view.
    */
  def upgrade(vertices: PKVertexRDDImpl[V], includeSrc: Boolean, includeDst: Boolean): Unit = {
    val shipSrc = includeSrc && !hasSrcId
    val shipDst = includeDst && !hasDstId
    if (shipSrc || shipDst) {
      val shippedVerts: RDD[(Int, VertexAttributeBlock[V])] =
        vertices
          .shipAttributes(shipSrc, shipDst)
          .setName(
            "ReplicatedVertexView.upgrade(%s, %s) - shippedVerts %s %s (broadcast)"
              .format(includeSrc, includeDst, shipSrc, shipDst)
          )
          .partitionBy(edges.partitioner.get)
      val newEdges = edges.withEdgePartitions(
        edges.zipEdgePartitions(shippedVerts) {
          (ePartIter, shippedVertsIter) =>
            ePartIter.map {
              case (pid, edgePartition) =>
                (pid, edgePartition.updateVertices(shippedVertsIter.flatMap(_._2.iterator)))
            }
        }
      )
      edges = newEdges
      hasSrcId = includeSrc
      hasDstId = includeDst
    }
  }

  /**
    * Return a new `ReplicatedVertexView` where vertex attributes in edge partition are updated using
    * `updates`. This ships a vertex attribute only to the edge partitions where it is in the
    * position(s) specified by the attribute shipping level.
    */
  def updateVertices(updates: VertexRDD[V]): PKReplicatedVertexView[V, E] = {
    val shippedVerts = updates
      .shipVertexAttributes(hasSrcId, hasDstId)
      .setName("ReplicatedVertexView.updateVertices - shippedVerts %s %s (broadcast)".format(hasSrcId, hasDstId))
      .partitionBy(edges.partitioner.get)

    val newEdges = edges.withEdgePartitions(
      edges.zipEdgePartitions(shippedVerts) {
        (ePartIter, shippedVertsIter) =>
          ePartIter.map {
            case (pid, edgePartition) =>
              (pid, edgePartition.updateVertices(shippedVertsIter.flatMap(_._2.iterator)))
          }
      }
    )

    new PKReplicatedVertexView(newEdges, hasSrcId, hasDstId)
  }
}
