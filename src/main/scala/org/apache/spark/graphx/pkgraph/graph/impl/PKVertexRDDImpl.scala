package org.apache.spark.graphx.pkgraph.graph.impl

import org.apache.spark.graphx.{EdgeRDD, PartitionID, VertexId, VertexRDD}
import org.apache.spark.{HashPartitioner, OneToOneDependency, Partition, Partitioner}
import org.apache.spark.graphx.impl.{ShippableVertexPartition, VertexAttributeBlock}
import org.apache.spark.graphx.pkgraph.graph.{PKEdgeRDD, PKVertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

// TODO: Check if we can create a new VertexRDD from the constructors in GraphX, this way we can avoid this class
private[pkgraph] class PKVertexRDDImpl[V](
    val vertexPartitions: RDD[PKVertexPartition[V]],
    val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
)(implicit override protected val vdTag: ClassTag[V])
    extends PKVertexRDD[V](vertexPartitions.context, List(new OneToOneDependency(vertexPartitions))) {

  require(vertexPartitions.partitioner.isDefined)

  override val partitioner: Option[Partitioner] = vertexPartitions.partitioner

  override protected def getPreferredLocations(s: Partition): Seq[String] = vertexPartitions.preferredLocations(s)

  override def setName(_name: String): this.type = {
    if (vertexPartitions.name != null) {
      vertexPartitions.setName(vertexPartitions.name + ", " + _name)
    } else {
      vertexPartitions.setName(_name)
    }
    this
  }

  setName("PKVertexRDD")

  /**
    * Persists the vertex partitions at the specified storage level, ignoring any existing target
    * storage level.
    */
  override def persist(newLevel: StorageLevel): this.type = {
    vertexPartitions.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = false): this.type = {
    vertexPartitions.unpersist(blocking)
    this
  }

  /**
    * Persists the vertex partitions at `targetStorageLevel`, which defaults to MEMORY_ONLY.
    */
  override def cache(): this.type = {
    vertexPartitions.persist(targetStorageLevel)
    this
  }

  override def getStorageLevel: StorageLevel = vertexPartitions.getStorageLevel

  override def checkpoint(): Unit = {
    vertexPartitions.checkpoint()
  }

  override def isCheckpointed: Boolean = {
    firstParent[ShippableVertexPartition[V]].isCheckpointed
  }

  override def getCheckpointFile: Option[String] = {
    vertexPartitions.getCheckpointFile
  }

  /** The number of vertices in the RDD. */
  /**
    * Get the number of vertices in this RDD.
    *
    * @return number of vertices
    */
  override def count(): Long = vertexPartitions.map(_.size.toLong).fold(0)(_ + _)

  /**
    * Construct a new [[VertexRDD]] that is indexed by only the visible vertices. The resulting
    * VertexRDD will be based on a different index and can no longer be quickly joined with this
    * RDD.
    *
    * @return re-indexed [[VertexRDD]]
    */
  override def reindex(): PKVertexRDDImpl[V] = withVertexPartitions(vertexPartitions.map(_.reindex()))

  /**
    * Apply the given user function to each vertex attribute in this RDD.
    *
    * @param f User function apply
    * @tparam V2 New type of vertex attributes
    * @return new [[VertexRDD]] with mapped attributes
    */
  override def mapValues[V2: ClassTag](f: V => V2): PKVertexRDDImpl[V2] =
    mapShippablePartitions(_.map((_, attr) => f(attr)))

  /**
    * Apply the given user function to each vertex in this RDD.
    *
    * @param f User function apply
    * @tparam V2 New type of vertex attributes
    * @return new [[VertexRDD]] with mapped attributes
    */
  override def mapValues[V2: ClassTag](f: (VertexId, V) => V2): PKVertexRDDImpl[V2] = mapShippablePartitions(_.map(f))

  override def minus(other: RDD[(VertexId, V)]): PKVertexRDDImpl[V] = {
    minus(aggregateUsingIndex(other, (a: V, _: V) => a))
  }

  override def minus(other: VertexRDD[V]): PKVertexRDDImpl[V] = {
    val otherRDD = other.asInstanceOf[PKVertexRDDImpl[V]]
    otherRDD match {
      case other: VertexRDD[_] if this.partitioner == other.partitioner =>
        withVertexPartitions[V](vertexPartitions.zipPartitions(other.vertexPartitions, preservesPartitioning = true) {
          (thisIter, otherIter) =>
            val thisPart = thisIter.next()
            val otherPart = otherIter.next()
            Iterator(thisPart.minus(otherPart))
        })
      case _ =>
        withVertexPartitions[V](
          vertexPartitions.zipPartitions(otherRDD.partitionBy(partitioner.get), preservesPartitioning = true) {
            (partIter, msgs) => partIter.map(_.minus(msgs))
          }
        )
    }
  }

  override def diff(other: RDD[(VertexId, V)]): PKVertexRDDImpl[V] = {
    diff(aggregateUsingIndex(other, (a: V, _: V) => a))
  }

  override def diff(other: VertexRDD[V]): PKVertexRDDImpl[V] = {
    val otherRDD = other.asInstanceOf[PKVertexRDDImpl[V]]
    val otherPartition = otherRDD match {
      case other: VertexRDD[_] if this.partitioner == other.partitioner =>
        other.vertexPartitions
      case _ => PKVertexRDDImpl(otherRDD.partitionBy(partitioner.get)).vertexPartitions
    }
    val newPartitionsRDD = vertexPartitions.zipPartitions(otherPartition, preservesPartitioning = true) {
      (thisIter, otherIter) =>
        val thisPart = thisIter.next()
        val otherPart = otherIter.next()
        Iterator(thisPart.diff(otherPart))
    }
    withVertexPartitions(newPartitionsRDD)
  }

  override def leftZipJoin[V2: ClassTag, V3: ClassTag](
      other: VertexRDD[V2]
  )(f: (VertexId, V, Option[V2]) => V3): PKVertexRDDImpl[V3] = {
    val otherRDD = other.asInstanceOf[PKVertexRDDImpl[V2]]
    val newPartitionsRDD = vertexPartitions.zipPartitions(otherRDD.vertexPartitions, preservesPartitioning = true) {
      (thisIter, otherIter) =>
        val thisPart = thisIter.next()
        val otherPart = otherIter.next()
        Iterator(thisPart.leftJoin(otherPart)(f))
    }
    withVertexPartitions(newPartitionsRDD)
  }

  override def leftJoin[V2: ClassTag, V3: ClassTag](
      other: RDD[(VertexId, V2)]
  )(f: (VertexId, V, Option[V2]) => V3): PKVertexRDDImpl[V3] = {
    // Test if the other vertex is a VertexRDD to choose the optimal join strategy.
    // If the other set is a VertexRDD then we use the much more efficient leftZipJoin
    other match {
      case other: VertexRDD[_] if this.partitioner == other.partitioner =>
        leftZipJoin(other)(f)
      case _ =>
        withVertexPartitions[V3](
          vertexPartitions.zipPartitions(other.partitionBy(this.partitioner.get), preservesPartitioning = true) {
            (partIter, msgs) => partIter.map(_.leftJoin(msgs)(f))
          }
        )
    }
  }

  override def innerZipJoin[U: ClassTag, V2: ClassTag](
      other: VertexRDD[U]
  )(f: (VertexId, V, U) => V2): PKVertexRDDImpl[V2] = {
    val otherRDD = other.asInstanceOf[PKVertexRDDImpl[U]]
    val newPartitionsRDD = vertexPartitions.zipPartitions(otherRDD.vertexPartitions, preservesPartitioning = true) {
      (thisIter, otherIter) =>
        val thisPart = thisIter.next()
        val otherPart = otherIter.next()
        Iterator(thisPart.innerJoin(otherPart)(f))
    }
    withVertexPartitions(newPartitionsRDD)
  }

  override def innerJoin[U: ClassTag, V2: ClassTag](
      other: RDD[(VertexId, U)]
  )(f: (VertexId, V, U) => V2): PKVertexRDDImpl[V2] = {
    // Test if the other vertex is a VertexRDD to choose the optimal join strategy.
    // If the other set is a VertexRDD then we use the much more efficient innerZipJoin
    other match {
      case other: VertexRDD[_] if this.partitioner == other.partitioner =>
        innerZipJoin(other)(f)
      case _ =>
        withVertexPartitions(
          vertexPartitions.zipPartitions(other.partitionBy(this.partitioner.get), preservesPartitioning = true) {
            (partIter, msgs) => partIter.map(_.innerJoin(msgs)(f))
          }
        )
    }
  }

  override def aggregateUsingIndex[V2: ClassTag](
      messages: RDD[(VertexId, V2)],
      reduceFunc: (V2, V2) => V2
  ): PKVertexRDDImpl[V2] = {
    val shuffled = messages.partitionBy(this.partitioner.get)
    val parts = vertexPartitions.zipPartitions(shuffled, preservesPartitioning = true) { (thisIter, msgIter) =>
      thisIter.map(_.aggregateUsingIndex(msgIter, reduceFunc))
    }
    withVertexPartitions[V2](parts)
  }

  override def reverseRoutingTables(): PKVertexRDDImpl[V] =
    mapShippablePartitions(vPart => vPart.withRoutingTable(vPart.routingTable.reverse))

  override def withEdges(edges: EdgeRDD[_]): PKVertexRDDImpl[V] = {
    val edgesRDD = edges.asInstanceOf[PKEdgeRDDImpl[V, _]]
    val routingTables = PKVertexRDDImpl.createRoutingTables(edgesRDD, this.partitioner.get)
    val partitions = vertexPartitions.zipPartitions(routingTables, preservesPartitioning = true) {
      (partIter, routingTableIter) =>
        val routingTable = if (routingTableIter.hasNext) routingTableIter.next() else PKRoutingTablePartition.empty
        partIter.map(_.withRoutingTable(routingTable))
    }
    withVertexPartitions(partitions)
  }

  /**
    * Generates an RDD of vertex attributes suitable for shipping to the edge partitions.
    *
    * @param shipSrc Include source vertex attributes
    * @param shipDst Include destination vertex attributes
    * @return new [[PKEdgeRDD]] with cached vertex attributes
    */
  def shipAttributes(shipSrc: Boolean, shipDst: Boolean): RDD[(PartitionID, VertexAttributeBlock[V])] = {
    vertexPartitions.mapPartitions(_.flatMap(_.shipVertexAttributes(shipSrc, shipDst)))
  }

  /**
    * Map each vertex partition in this RDD using the given user function.
    *
    * @param f User function to apply
    * @tparam V2 New type of vertex attributes
    * @return
    */
  def mapShippablePartitions[V2: ClassTag](
      f: PKVertexPartition[V] => PKVertexPartition[V2]
  ): PKVertexRDDImpl[V2] = {
    val newPartitionsRDD = vertexPartitions.mapPartitions(_.map(f), preservesPartitioning = true)
    withVertexPartitions(newPartitionsRDD)
  }

  /**
    * Creates a new [[PKVertexRDD]] with the given vertex partitions, using the same storage level.
    *
    * @param partitionsRDD New vertex partitions
    * @tparam V2 New type of vertex attribute
    * @return new [[PKVertexRDD]] with given vertex partitions
    */
  private def withVertexPartitions[V2: ClassTag](
      partitionsRDD: RDD[PKVertexPartition[V2]]
  ): PKVertexRDDImpl[V2] = {
    new PKVertexRDDImpl(partitionsRDD, targetStorageLevel)
  }
}

object PKVertexRDDImpl {

  /**
    * Constructs a standalone [[PKVertexRDDImpl]] (one that is not set up for efficient joins with an
    * [[EdgeRDD]]) from an RDD of vertex-attribute pairs. Duplicate entries are removed arbitrarily.
    *
    * @tparam V the vertex attribute type
    *
    * @param vertices the collection of vertex-attribute pairs
    */
  def apply[V: ClassTag](vertices: RDD[(VertexId, V)]): PKVertexRDDImpl[V] = {
    val vPartitioned: RDD[(VertexId, V)] = vertices.partitioner match {
      case Some(_) => vertices
      case None    => vertices.partitionBy(new HashPartitioner(vertices.partitions.length))
    }
    val vertexPartitions =
      vPartitioned.mapPartitions(iter => Iterator(PKVertexPartition(iter)), preservesPartitioning = true)
    new PKVertexRDDImpl[V](vertexPartitions)
  }

  /**
    * Constructs a [[PKVertexRDDImpl]] from an RDD of vertex-attribute pairs. Duplicate vertex entries are
    * removed arbitrarily. The resulting `VertexRDD` will be joinable with `edges`, and any missing
    * vertices referred to by `edges` will be created with the attribute `defaultVal`.
    *
    * @tparam V the vertex attribute type
    *
    * @param vertices the collection of vertex-attribute pairs
    * @param edges the [[EdgeRDD]] that these vertices may be joined with
    * @param defaultVal the vertex attribute to use when creating missing vertices
    */
  def apply[V: ClassTag](
      vertices: RDD[(VertexId, V)],
      edges: PKEdgeRDDImpl[V, _],
      defaultVal: V
  ): PKVertexRDDImpl[V] = {
    PKVertexRDDImpl(vertices, edges, defaultVal, (a, _) => a)
  }

  /**
    * Constructs a [[PKVertexRDDImpl]] from an RDD of vertex-attribute pairs. Duplicate vertex entries are
    * merged using `mergeFunc`. The resulting `VertexRDD` will be joinable with `edges`, and any
    * missing vertices referred to by `edges` will be created with the attribute `defaultVal`.
    *
    * @tparam V the vertex attribute type
    *
    * @param vertices the collection of vertex-attribute pairs
    * @param edges the [[EdgeRDD]] that these vertices may be joined with
    * @param defaultVal the vertex attribute to use when creating missing vertices
    * @param mergeFunc the commutative, associative duplicate vertex attribute merge function
    */
  def apply[V: ClassTag](
      vertices: RDD[(VertexId, V)],
      edges: PKEdgeRDDImpl[V, _],
      defaultVal: V,
      mergeFunc: (V, V) => V
  ): PKVertexRDDImpl[V] = {
    val vPartitioned: RDD[(VertexId, V)] = vertices.partitioner match {
      case Some(_) => vertices
      case None    => vertices.partitionBy(new HashPartitioner(vertices.partitions.length))
    }
    val routingTables = createRoutingTables(edges, vPartitioned.partitioner.get)
    val vertexPartitions = vPartitioned.zipPartitions(routingTables, preservesPartitioning = true) {
      (vertexIter, routingTableIter) =>
        val routingTable = if (routingTableIter.hasNext) routingTableIter.next() else PKRoutingTablePartition.empty
        Iterator(PKVertexPartition(vertexIter, routingTable, defaultVal, mergeFunc))
    }
    new PKVertexRDDImpl(vertexPartitions)
  }

  /**
    * Constructs a [[PKVertexRDD]] containing all vertices referred to in `edges`. The vertices will be
    * created with the attribute `defaultVal`. The resulting [[PKVertexRDD]] will be joinable with
    * `edges`.
    *
    * @tparam V the vertex attribute type
    *
    * @param edges the [[PKEdgeRDD]] referring to the vertices to create
    * @param numPartitions the desired number of partitions for the resulting `VertexRDD`
    * @param defaultVal the vertex attribute to use when creating missing vertices
    */
  def fromEdges[V: ClassTag](edges: PKEdgeRDDImpl[V, _], numPartitions: Int, defaultVal: V): PKVertexRDD[V] = {
    val routingTables = createRoutingTables(edges, new HashPartitioner(numPartitions))
    val vertexPartitions = routingTables.mapPartitions(
      { routingTableIter =>
        val routingTable = if (routingTableIter.hasNext) routingTableIter.next() else PKRoutingTablePartition.empty
        Iterator(PKVertexPartition(Iterator.empty, routingTable, defaultVal))
      },
      preservesPartitioning = true
    )
    new PKVertexRDDImpl(vertexPartitions)
  }

  private def createRoutingTables[V: ClassTag](
      edges: PKEdgeRDDImpl[V, _],
      partitioner: Partitioner
  ): RDD[PKRoutingTablePartition] = {
    // Determine which vertices each edge partition needs by creating a mapping from vid to pid.
    val vid2pid = edges.edgePartitions
      .mapPartitions(_.flatMap(Function.tupled(PKRoutingTablePartition.edgePartitionToMsgs)))
      .setName("PKVertexRDD.createRoutingTables - vid2pid (aggregation)")

    val numEdgePartitions = edges.partitions.length
    vid2pid
      .partitionBy(partitioner)
      .mapPartitions(
        iter => Iterator(PKRoutingTablePartition.fromMsgs(numEdgePartitions, iter)),
        preservesPartitioning = true
      )
  }
}
