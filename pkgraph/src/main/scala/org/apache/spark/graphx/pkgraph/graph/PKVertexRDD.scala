package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.graphx.impl.RoutingTablePartition.RoutingTableMessage
import org.apache.spark.graphx.impl.{RoutingTablePartition, ShippableVertexPartition, VertexRDDImpl}
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.graphx.{EdgeRDD, PartitionID, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner}

import scala.reflect.ClassTag

object PKVertexRDD {

  /**
    * Constructs a `VertexRDD` from an RDD of vertex-attribute pairs. Duplicate vertex entries are
    * removed arbitrarily. The resulting `VertexRDD` will be joinable with `edges`, and any missing
    * vertices referred to by `edges` will be created with the attribute `defaultVal`.
    *
    * @tparam V the vertex attribute type
    *
    * @param vertices the collection of vertex-attribute pairs
    * @param edges the [[PKEdgeRDD]] that these vertices may be joined with
    * @param defaultVal the vertex attribute to use when creating missing vertices
    */
  def apply[V: ClassTag](vertices: RDD[(VertexId, V)], edges: PKEdgeRDD[V, _], defaultVal: V): VertexRDD[V] = {
    PKVertexRDD(vertices, edges, defaultVal, (a, _) => a)
  }

  /**
    * Constructs a `VertexRDD` from an RDD of vertex-attribute pairs. Duplicate vertex entries are
    * merged using `mergeFunc`. The resulting `VertexRDD` will be joinable with `edges`, and any
    * missing vertices referred to by `edges` will be created with the attribute `defaultVal`.
    *
    * @tparam V the vertex attribute type
    *
    * @param vertices the collection of vertex-attribute pairs
    * @param edges the [[PKEdgeRDD]] that these vertices may be joined with
    * @param defaultVal the vertex attribute to use when creating missing vertices
    * @param mergeFunc the commutative, associative duplicate vertex attribute merge function
    */
  def apply[V: ClassTag](
      vertices: RDD[(VertexId, V)],
      edges: PKEdgeRDD[V, _],
      defaultVal: V,
      mergeFunc: (V, V) => V
  ): VertexRDD[V] = {
    val vPartitioned: RDD[(VertexId, V)] = vertices.partitioner match {
      case Some(_) => vertices
      case None    => vertices.partitionBy(new HashPartitioner(vertices.partitions.length))
    }
    val routingTables = createRoutingTables(edges, vPartitioned.partitioner.get)
    val vertexPartitions = vPartitioned.zipPartitions(routingTables, preservesPartitioning = true) {
      (vertexIter, routingTableIter) =>
        val routingTable =
          if (routingTableIter.hasNext) routingTableIter.next() else RoutingTablePartition.empty
        Iterator(ShippableVertexPartition(vertexIter, routingTable, defaultVal, mergeFunc))
    }
    new VertexRDDImpl(vertexPartitions)
  }

  /**
    * Constructs a `VertexRDD` containing all vertices referred to in `edges`. The vertices will be
    * created with the attribute `defaultVal`. The resulting `VertexRDD` will be joinable with
    * `edges`.
    *
    * @tparam V the vertex attribute type
    *
    * @param edges the [[PKEdgeRDD]] referring to the vertices to create
    * @param numPartitions the desired number of partitions for the resulting `VertexRDD`
    * @param defaultVal the vertex attribute to use when creating missing vertices
    */
  def fromEdges[V: ClassTag](edges: PKEdgeRDD[V, _], numPartitions: Int, defaultVal: V): VertexRDD[V] = {
    val routingTables = createRoutingTables(edges, new HashPartitioner(numPartitions))
    val vertexPartitions = routingTables.mapPartitions(
      { routingTableIter =>
        val routingTable = if (routingTableIter.hasNext) routingTableIter.next() else RoutingTablePartition.empty
        Iterator(ShippableVertexPartition(Iterator.empty, routingTable, defaultVal))
      },
      preservesPartitioning = true
    )
    new VertexRDDImpl(vertexPartitions)
  }

  /**
    * Prepares an VertexRDD for efficient joins with the given EdgeRDD.
    *
    * @param vertices  VertexRDD to prepare
    * @param edges   Edges to join with
    * @return new VertexRDD
    */
  def withEdges[V: ClassTag](vertices: VertexRDDImpl[V], edges: PKEdgeRDD[_, _]): VertexRDD[V] = {
    val routingTables = createRoutingTables(edges, vertices.partitioner.get)
    val vertexPartitions = vertices.partitionsRDD.zipPartitions(routingTables, preservesPartitioning = true) {
      (partIter, routingTableIter) =>
        val routingTable = if (routingTableIter.hasNext) routingTableIter.next() else RoutingTablePartition.empty
        partIter.map(_.withRoutingTable(routingTable))
    }
    new VertexRDDImpl(vertexPartitions, vertices.targetStorageLevel)
  }

  private def createRoutingTables(
      edges: PKEdgeRDD[_, _],
      vertexPartitioner: Partitioner
  ): RDD[RoutingTablePartition] = {
    // Determine which vertices each edge partition needs by creating a mapping from vid to pid.
    val vid2pid = edges.edgePartitions
      .mapPartitions(_.flatMap(Function.tupled(edgePartitionToMsgs)))
      .setName("VertexRDD.createRoutingTables - vid2pid (aggregation)")

    val numEdgePartitions = edges.partitions.length
    vid2pid
      .partitionBy(vertexPartitioner)
      .mapPartitions(
        iter => Iterator(RoutingTablePartition.fromMsgs(numEdgePartitions, iter)),
        preservesPartitioning = true
      )
  }

  /** Generate a `RoutingTableMessage` for each vertex referenced in `edgePartition`. */
  private def edgePartitionToMsgs(pid: PartitionID, partition: PKEdgePartition[_, _]): Iterator[RoutingTableMessage] = {
    // Determine which positions each vertex id appears in using a map where the low 2 bits
    // represent src and dst
    val map = new GraphXPrimitiveKeyOpenHashMap[VertexId, Byte]
    partition.iterator.foreach { e =>
      map.changeValue(e.srcId, 0x1, (b: Byte) => (b | 0x1).toByte)
      map.changeValue(e.dstId, 0x2, (b: Byte) => (b | 0x2).toByte)
    }
    map.iterator.map { vidAndPosition =>
      val vid = vidAndPosition._1
      val position = vidAndPosition._2
      toRoutingTableMessage(vid, pid, position)
    }
  }

  private def toRoutingTableMessage(vid: VertexId, pid: PartitionID, position: Byte): RoutingTableMessage = {
    val positionUpper2 = position << 30
    val pidLower30 = pid & 0x3fffffff
    (vid, positionUpper2 | pidLower30)
  }
}
