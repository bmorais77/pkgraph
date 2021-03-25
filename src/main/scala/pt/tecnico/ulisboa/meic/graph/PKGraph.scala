package pt.tecnico.ulisboa.meic.graph

import org.apache.spark.HashPartitioner
import org.apache.spark.graphx.impl.{EdgeActiveness, EdgeRDDImpl, GraphImpl, ReplicatedVertexView}
import org.apache.spark.graphx.vertices.PKVertexRDD
import org.apache.spark.graphx.{
  Edge,
  EdgeContext,
  EdgeDirection,
  EdgeRDD,
  EdgeTriplet,
  Graph,
  PartitionID,
  PartitionStrategy,
  TripletFields,
  VertexId,
  VertexRDD
}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import pt.tecnico.ulisboa.meic.graph.impl.{PKEdgePartitionBuilder, PKEdgeRDD, PKReplicatedVertexView}

import scala.reflect.{ClassTag, classTag}

class PKGraph[V: ClassTag, E: ClassTag] private (
    override val vertices: VertexRDD[V],
    val replicatedVertexView: PKReplicatedVertexView[V, E]
) extends Graph[V, E] {
  override val edges: PKEdgeRDD[V, E] = replicatedVertexView.edges

  /** Return an RDD that brings edges together with their source and destination vertices. */
  @transient override lazy val triplets: RDD[EdgeTriplet[V, E]] = {
    replicatedVertexView.upgrade(vertices, includeSrc = true, includeDst = true)
    replicatedVertexView.edges.edgePartitions.mapPartitions(_.flatMap {
      case (_, part) => part.tripletIterator()
    })
  }

  override def persist(newLevel: StorageLevel): Graph[V, E] = {
    vertices.persist(newLevel)
    replicatedVertexView.edges.persist(newLevel)
    this
  }

  override def cache(): Graph[V, E] = {
    vertices.cache()
    replicatedVertexView.edges.cache()
    this
  }

  override def checkpoint(): Unit = {
    vertices.checkpoint()
    replicatedVertexView.edges.checkpoint()
  }

  override def isCheckpointed: Boolean = {
    vertices.isCheckpointed && replicatedVertexView.edges.isCheckpointed
  }

  override def getCheckpointFiles: Seq[String] = {
    Seq(vertices.getCheckpointFile, replicatedVertexView.edges.getCheckpointFile).flatMap {
      case Some(path) => Seq(path)
      case None       => Seq.empty
    }
  }

  override def unpersist(blocking: Boolean = false): Graph[V, E] = {
    unpersistVertices(blocking)
    replicatedVertexView.edges.unpersist(blocking)
    this
  }

  override def unpersistVertices(blocking: Boolean = false): Graph[V, E] = {
    vertices.unpersist(blocking)
    // TODO: unpersist the replicated vertices in `replicatedVertexView` but leave the edges alone
    this
  }

  override def partitionBy(partitionStrategy: PartitionStrategy): Graph[V, E] = {
    partitionBy(partitionStrategy, edges.partitions.length)
  }

  override def partitionBy(partitionStrategy: PartitionStrategy, numPartitions: Int): Graph[V, E] = {
    val vTag = classTag[V]
    val eTag = classTag[E]
    val newEdges = edges
      .withEdgePartitions(
        edges
          .map { e =>
            val part: PartitionID = partitionStrategy.getPartition(e.srcId, e.dstId, numPartitions)
            (part, (e.srcId, e.dstId, e.attr))
          }
          .partitionBy(new HashPartitioner(numPartitions))
          .mapPartitionsWithIndex(
            { (pid: Int, iter: Iterator[(PartitionID, (VertexId, VertexId, E))]) =>
              val builder = new PKEdgePartitionBuilder[V, E]()(vTag, eTag)
              iter.foreach { message =>
                val data = message._2
                builder.add(data._1, data._2, data._3)
              }
              val edgePartition = builder.build
              Iterator((pid, edgePartition))
            },
            preservesPartitioning = true
          )
      )
      .cache()

    // TODO: withEdges calls partitionsRDD of PKEdgeRDD
    PKGraph.fromExistingRDDs(vertices.withEdges(newEdges), newEdges)
  }

  override def reverse: Graph[V, E] = {
    new PKGraph(vertices.reverseRoutingTables(), replicatedVertexView.reverse())
  }

  override def mapVertices[V2: ClassTag](f: (VertexId, V) => V2)(implicit eq: V =:= V2 = null): Graph[V2, E] = {
    // The implicit parameter eq will be populated by the compiler if VD and VD2 are equal, and left
    // null if not
    if (eq != null) {
      vertices.cache()
      // The map preserves type, so we can use incremental replication
      val newVerts = vertices.mapValues(f).cache()
      val changedVerts = vertices.asInstanceOf[VertexRDD[V2]].diff(newVerts)
      val newReplicatedVertexView = replicatedVertexView
        .asInstanceOf[PKReplicatedVertexView[V2, E]]
        .updateVertices(changedVerts)
      this.asInstanceOf[PKGraph[V2, E]]
      new PKGraph(newVerts, newReplicatedVertexView)
    } else {
      // The map does not preserve type, so we must re-replicate all vertices
      this.asInstanceOf[PKGraph[V2, E]]
      // TODO: GraphImpl(vertices.mapVertexPartitions(_.map(f)), replicatedVertexView.edges)
    }
  }

  override def mapEdges[E2: ClassTag](f: (PartitionID, Iterator[Edge[E]]) => Iterator[E2]): Graph[V, E2] = {
    val newEdges = replicatedVertexView.edges
      .mapEdgePartitions((pid, part) => part.map(f(pid, part.iterator)))
    this.asInstanceOf[PKGraph[V, E2]]
    // TODO: new GraphImpl(vertices, replicatedVertexView.withEdges(newEdges))
  }

  override def mapTriplets[E2: ClassTag](
      f: (PartitionID, Iterator[EdgeTriplet[V, E]]) => Iterator[E2],
      tripletFields: TripletFields
  ): Graph[V, E2] = {
    vertices.cache()
    replicatedVertexView.upgrade(vertices, tripletFields.useSrc, tripletFields.useDst)
    val newEdges = replicatedVertexView.edges.mapEdgePartitions { (pid, part) =>
      part.map(f(pid, part.tripletIterator(tripletFields.useSrc, tripletFields.useDst)))
    }
    this.asInstanceOf[PKGraph[V, E2]]
    // TODO: new GraphImpl(vertices, replicatedVertexView.withEdges(newEdges))
  }

  override def subgraph(
      epred: EdgeTriplet[V, E] => Boolean = x => true,
      vpred: (VertexId, V) => Boolean = (a, b) => true
  ): Graph[V, E] = {
    vertices.cache()
    // Filter the vertices, reusing the partitioner and the index from this graph
    val newVerts = vertices.mapPKVertexPartitions(_.filter(vpred))
    // Filter the triplets. We must always upgrade the triplet view fully because vpred always runs
    // on both src and dst vertices
    replicatedVertexView.upgrade(vertices, includeSrc = true, includeDst = true)
    val newEdges = replicatedVertexView.edges.filter(epred, vpred)
    this
    // TODO: new GraphImpl(newVerts, replicatedVertexView.withEdges(newEdges))
  }

  override def mask[V2: ClassTag, E2: ClassTag](other: Graph[V2, E2]): Graph[V, E] = {
    val newVerts = vertices.innerJoin(other.vertices) { (vid, v, w) => v }
    val newEdges = replicatedVertexView.edges.innerJoin(other.edges) { (src, dst, v, w) => v }
    this
    // TODO: new GraphImpl(newVerts, replicatedVertexView.withEdges(newEdges))
  }

  override def groupEdges(merge: (E, E) => E): Graph[V, E] = {
    val newEdges = replicatedVertexView.edges.mapEdgePartitions((_, part) => part.groupEdges(merge))
    this
    // TODO: new GraphImpl(vertices, replicatedVertexView.withEdges(newEdges))
  }

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  // Lower level transformation methods
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  override def aggregateMessagesWithActiveSet[A: ClassTag](
      sendMsg: EdgeContext[V, E, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields,
      activeSetOpt: Option[(VertexRDD[_], EdgeDirection)]
  ): VertexRDD[A] = {

    vertices.cache()
    // For each vertex, replicate its attribute only to partitions where it is
    // in the relevant position in an edge.
    replicatedVertexView.upgrade(vertices, tripletFields.useSrc, tripletFields.useDst)
    val activeDirectionOpt = activeSetOpt.map(_._2)

    // Map and combine.
    val preAgg = replicatedVertexView.edges.edgePartitions
      .mapPartitions(_.flatMap {
        case (_, edgePartition) =>
          // Choose scan method
          activeDirectionOpt match {
            case Some(EdgeDirection.Both) =>
              edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields, EdgeActiveness.Both)
            case Some(EdgeDirection.Either) =>
              // TODO: Because we only have a clustered index on the source vertex ID, we can't filter
              // the index here. Instead we have to scan all edges and then do the filter.
              edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields, EdgeActiveness.Either)
            case Some(EdgeDirection.Out) =>
              edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields, EdgeActiveness.SrcOnly)
            case Some(EdgeDirection.In) =>
              edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields, EdgeActiveness.DstOnly)
            case _ => // None
              edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields, EdgeActiveness.Neither)
          }
      })
      .setName("GraphImpl.aggregateMessages - preAgg")

    // do the final reduction reusing the index map
    vertices.aggregateUsingIndex(preAgg, mergeMsg)
  }

  override def outerJoinVertices[U: ClassTag, V2: ClassTag](
      other: RDD[(VertexId, U)]
  )(updateF: (VertexId, V, Option[U]) => V2)(implicit eq: V =:= V2 = null): Graph[V2, E] = {
    // The implicit parameter eq will be populated by the compiler if VD and VD2 are equal, and left
    // null if not
    if (eq != null) {
      vertices.cache()
      // updateF preserves type, so we can use incremental replication
      val newVerts = vertices.leftJoin(other)(updateF).cache()
      val changedVerts = vertices.asInstanceOf[VertexRDD[V2]].diff(newVerts)
      val newReplicatedVertexView = replicatedVertexView
        .asInstanceOf[PKReplicatedVertexView[V2, E]]
        .updateVertices(changedVerts)
      this.asInstanceOf[Graph[V2, E]]
      new PKGraph(newVerts, newReplicatedVertexView)
    } else {
      // updateF does not preserve type, so we must re-replicate all vertices
      val newVerts = vertices.leftJoin(other)(updateF)
      this.asInstanceOf[Graph[V2, E]]
      // TODO: GraphImpl(newVerts, replicatedVertexView.edges)
    }
  }
}

object PKGraph {

  /**
    * Create a graph from a [[VertexRDD]] and an [[PKEdgeRDD]] with the same replicated vertex type as the
    * vertices. The [[VertexRDD]] must already be set up for efficient joins with the [[PKEdgeRDD]] by calling
    * `VertexRDD.withEdges` or an appropriate VertexRDD constructor.
    *
    * @param vertices RDD with vertices
    * @param edges RDD with edges
    * @tparam V Vertex attribute type
    * @tparam E Edge attribute type
    * @return new [[PKGraph]] from existing vertex and edge RDDs
    */
  def fromExistingRDDs[V: ClassTag, E: ClassTag](vertices: VertexRDD[V], edges: PKEdgeRDD[V, E]): PKGraph[V, E] = {
    new PKGraph(vertices, new PKReplicatedVertexView(edges))
  }
}
