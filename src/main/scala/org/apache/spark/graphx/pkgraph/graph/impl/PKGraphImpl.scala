package org.apache.spark.graphx.pkgraph.graph.impl

import org.apache.spark.HashPartitioner
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.EdgeActiveness
import org.apache.spark.graphx.pkgraph.graph.{PKGraph, PKVertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.{ClassTag, classTag}

class PKGraphImpl[V: ClassTag, E: ClassTag] private (
    override val vertices: PKVertexRDDImpl[V],
    val replicatedVertexView: PKReplicatedVertexView[V, E]
) extends PKGraph[V, E] {
  override val edges: PKEdgeRDDImpl[V, E] = replicatedVertexView.edges

  /**
    * Return an RDD that brings edges together with their source and destination vertices attributes.
    */
  @transient override lazy val triplets: RDD[EdgeTriplet[V, E]] = {
    replicatedVertexView.upgrade(vertices, includeSrc = true, includeDst = true)
    replicatedVertexView.edges.edgePartitions.mapPartitions(_.flatMap {
      case (_, part) => part.tripletIterator()
    })
  }

  /**
    * Caches the vertices and edges associated with this graph at the specified storage level,
    * ignoring any target storage levels previously set.
    *
    * @param newLevel the level at which to cache the graph.
    *
    * @return A reference to this graph for convenience.
    */
  override def persist(newLevel: StorageLevel): Graph[V, E] = {
    vertices.persist(newLevel)
    replicatedVertexView.edges.persist(newLevel)
    this
  }

  /**
    * Caches the vertices and edges associated with this graph at the previously-specified target
    * storage levels, which default to `MEMORY_ONLY`. This is used to pin a graph in memory enabling
    * multiple queries to reuse the same construction process.
    *
    * @return A reference to this graph for convenience.
    */
  override def cache(): Graph[V, E] = {
    vertices.cache()
    replicatedVertexView.edges.cache()
    this
  }

  /**
    * Mark this Graph for checkpointing. It will be saved to a file inside the checkpoint
    * directory set with SparkContext.setCheckpointDir() and all references to its parent
    * RDDs will be removed. It is strongly recommended that this Graph is persisted in
    * memory, otherwise saving it on a file will require recomputation.
    */
  override def checkpoint(): Unit = {
    vertices.checkpoint()
    replicatedVertexView.edges.checkpoint()
  }

  /**
    * Return whether this Graph has been checkpointed or not.
    * This returns true iff both the vertices RDD and edges RDD have been checkpointed.
    *
    * @return true if graph is checkpointed, false otherwise
    */
  override def isCheckpointed: Boolean = {
    vertices.isCheckpointed && replicatedVertexView.edges.isCheckpointed
  }

  /**
    * Gets the name of the files to which this Graph was checkpointed.
    * (The vertices RDD and edges RDD are checkpointed separately.)
    *
    * @return sequence with vertices checkpoint file and edge checkpoint file
    */
  override def getCheckpointFiles: Seq[String] = {
    Seq(vertices.getCheckpointFile, replicatedVertexView.edges.getCheckpointFile).flatMap {
      case Some(path) => Seq(path)
      case None       => Seq.empty
    }
  }

  /**
    * Uncaches both vertices and edges of this graph. This is useful in iterative algorithms that
    * build a new graph in each iteration.
    *
    * @param blocking Whether to block until all data is unpersisted (default: false)
    * @return A reference to this graph for convenience.
    */
  override def unpersist(blocking: Boolean = false): Graph[V, E] = {
    unpersistVertices(blocking)
    replicatedVertexView.edges.unpersist(blocking)
    this
  }

  /**
    * Uncaches only the vertices of this graph, leaving the edges alone. This is useful in iterative
    * algorithms that modify the vertex attributes but reuse the edges. This method can be used to
    * uncache the vertex attributes of previous iterations once they are no longer needed, improving
    * GC performance.
    *
    * @param blocking Whether to block until all data is unpersisted (default: false)
    * @return A reference to this graph for convenience.
    */
  override def unpersistVertices(blocking: Boolean = false): Graph[V, E] = {
    vertices.unpersist(blocking)
    // TODO: unpersist the replicated vertices in `replicatedVertexView` but leave the edges alone
    this
  }

  /**
    * Repartitions the edges in the graph according to the given `partitionStrategy`.
    *
    * @param partitionStrategy the partitioning strategy to use when partitioning the edges in the graph.
    * @return graph partitioned with the given [[PartitionStrategy]]
    */
  override def partitionBy(partitionStrategy: PartitionStrategy): PKGraphImpl[V, E] = {
    partitionBy(partitionStrategy, edges.partitions.length)
  }

  /**
    * Repartitions the edges in the graph according to the given `partitionStrategy`.
    *
    * @param partitionStrategy the partitioning strategy to use when partitioning the edges in the graph.
    * @param numPartitions the number of edge partitions in the new graph.
    * @return graph partitioned with the given [[PartitionStrategy]] and with the given number of partitions
    */
  override def partitionBy(partitionStrategy: PartitionStrategy, numPartitions: Int): PKGraphImpl[V, E] = {
    val vTag = classTag[V]
    val eTag = classTag[E]
    val partitions = edges
      .map { e =>
        val part: PartitionID = partitionStrategy.getPartition(e.srcId, e.dstId, numPartitions)
        (part, (e.srcId, e.dstId, e.attr))
      }
      .partitionBy(new HashPartitioner(numPartitions))
      .mapPartitionsWithIndex(
        { (pid: Int, iter: Iterator[(PartitionID, (VertexId, VertexId, E))]) =>
          // TODO: Partition builder defaults to K=2, maybe this should be an option on the graph constructor
          val builder = PKEdgePartitionBuilder[V, E](2)(vTag, eTag)
          iter.foreach { message =>
            val data = message._2
            builder.add(data._1, data._2, data._3)
          }
          val edgePartition = builder.build
          Iterator((pid, edgePartition))
        },
        preservesPartitioning = true
      )

    val newEdges = edges.withEdgePartitions(partitions).cache()
    PKGraphImpl.fromExistingRDDs(vertices.withEdges(newEdges), newEdges)
  }

  /**
    * Transforms each vertex attribute in the graph using the map function.
    *
    * @note The new graph has the same structure.  As a consequence the underlying index structures
    * can be reused.
    *
    * @param f the function from a vertex object to a new vertex value
    * @tparam V2 the new vertex data type
    * @example We might use this operation to change the vertex values
    * from one type to another to initialize an algorithm.
    * {{{
    * val rawGraph: Graph[(), ()] = Graph.textFile("hdfs://file")
    * val root = 42
    * var bfsGraph = rawGraph.mapVertices[Int]((vid, data) => if (vid == root) 0 else Math.MaxValue)
    * }}}
    *
    */
  override def mapVertices[V2: ClassTag](f: (VertexId, V) => V2)(implicit eq: V =:= V2 = null): Graph[V2, E] = {
    // The implicit parameter eq will be populated by the compiler if VD and VD2 are equal, and left
    // null if not
    if (eq != null) {
      vertices.cache()
      // The map preserves type, so we can use incremental replication
      val newVerts = vertices.mapValues(f).cache()
      val changedVerts = vertices.asInstanceOf[PKVertexRDDImpl[V2]].diff(newVerts)
      val newReplicatedVertexView = replicatedVertexView
        .asInstanceOf[PKReplicatedVertexView[V2, E]]
        .updateVertices(changedVerts)
      new PKGraphImpl(newVerts, newReplicatedVertexView)
    } else {
      // The map does not preserve type, so we must re-replicate all vertices
      // TODO: Cached attributes in edge partitions are not updated?
      PKGraphImpl(vertices.mapValues(f), replicatedVertexView.edges.asInstanceOf[PKEdgeRDDImpl[V2, E]])
    }
  }

  /**
    * Transforms each edge attribute in the graph using the map function.  The map function is not
    * passed the vertex value for the vertices adjacent to the edge.  If vertex values are desired,
    * use `mapTriplets`.
    *
    * @note This graph is not changed and that the new graph has the
    * same structure.  As a consequence the underlying index structures
    * can be reused.
    *
    * @param f the function from an edge object to a new edge value.
    * @tparam E2 the new edge data type
    * @example This function might be used to initialize edge
    * attributes.
    *
    * @return new graph with edges mapped with the given user function
    */
  override def mapEdges[E2: ClassTag](f: (PartitionID, Iterator[Edge[E]]) => Iterator[E2]): Graph[V, E2] = {
    val newEdges = replicatedVertexView.edges.mapEdgePartitions[V, E2]((pid, part) => part.map(f(pid, part.iterator)))
    new PKGraphImpl[V, E2](vertices, replicatedVertexView.withEdges(newEdges))
  }

  /**
    * Transforms each edge attribute using the map function, passing it a whole partition at a
    * time. The map function is given an iterator over edges within a logical partition as well as
    * the partition's ID, and it should return a new iterator over the new values of each edge. The
    * new iterator's elements must correspond one-to-one with the old iterator's elements. If
    * adjacent vertex values are desired, use `mapTriplets`.
    *
    * @note This does not change the structure of the
    * graph or modify the values of this graph.  As a consequence
    * the underlying index structures can be reused.
    *
    * @param f a function that takes a partition id and an iterator
    * over all the edges in the partition, and must return an iterator over
    * the new values for each edge in the order of the input iterator
    * @param tripletFields Fields to include alongside the edge
    * @tparam E2 the new edge data type
    *
    * @return new graph with edges mapped with the given user function
    */
  override def mapTriplets[E2: ClassTag](
      f: (PartitionID, Iterator[EdgeTriplet[V, E]]) => Iterator[E2],
      tripletFields: TripletFields
  ): Graph[V, E2] = {
    vertices.cache()
    replicatedVertexView.upgrade(vertices, tripletFields.useSrc, tripletFields.useDst)
    val newEdges = replicatedVertexView.edges.mapEdgePartitions[V, E2] { (pid, part) =>
      part.map(f(pid, part.tripletIterator(tripletFields.useSrc, tripletFields.useDst)))
    }
    new PKGraphImpl[V, E2](vertices, replicatedVertexView.withEdges(newEdges))
  }

  /**
    * Reverses all edges in the graph.  If this graph contains an edge from a to b then the returned
    * graph contains an edge from b to a.
    *
    * @return new graph with reversed edges
    */
  override def reverse: Graph[V, E] = new PKGraphImpl(vertices.reverseRoutingTables(), replicatedVertexView.reverse())

  /**
    * Restricts the graph to only the vertices and edges satisfying the predicates. The resulting
    * subgraph satisfies
    *
    * {{{
    * V' = {v : for all v in V where vpred(v)}
    * E' = {(u,v): for all (u,v) in E where epred((u,v)) && vpred(u) && vpred(v)}
    * }}}
    *
    * @param epred the edge predicate, which takes a triplet and
    * evaluates to true if the edge is to remain in the subgraph.  Note
    * that only edges where both vertices satisfy the vertex
    * predicate are considered.
    *
    * @param vpred the vertex predicate, which takes a vertex object and
    * evaluates to true if the vertex is to be included in the subgraph
    *
    * @return the subgraph containing only the vertices and edges that
    * satisfy the predicates
    */
  override def subgraph(
      epred: EdgeTriplet[V, E] => Boolean = _ => true,
      vpred: (VertexId, V) => Boolean = (_, _) => true
  ): Graph[V, E] = {
    vertices.cache()
    // Filter the vertices, reusing the partitioner and the index from this graph
    val newVerts = vertices.mapPKVertexPartitions(_.filter(vpred))
    // Filter the triplets. We must always upgrade the triplet view fully because vpred always runs
    // on both src and dst vertices
    replicatedVertexView.upgrade(vertices, includeSrc = true, includeDst = true)
    val newEdges = replicatedVertexView.edges.filter(epred, vpred)
    new PKGraphImpl[V, E](newVerts, replicatedVertexView.withEdges(newEdges))
  }

  /**
    * Restricts the graph to only the vertices and edges that are also in `other`, but keeps the
    * attributes from this graph.
    * @param other the graph to project this graph onto
    * @tparam V2 `other` type of vertex attributes
    * @tparam E2 `other` type of edge attributes
    *
    * @return a graph with vertices and edges that exist in both the current graph and `other`,
    * with vertex and edge data from the current graph
    */
  override def mask[V2: ClassTag, E2: ClassTag](other: Graph[V2, E2]): Graph[V, E] = {
    val newVerts = vertices.innerJoin(other.vertices) { (_, v, _) => v }
    val newEdges = replicatedVertexView.edges.innerJoin(other.edges) { (_, _, v, _) => v }
    new PKGraphImpl(newVerts, replicatedVertexView.withEdges(newEdges))
  }

  /**
    * Merges multiple edges between two vertices into a single edge. For correct results, the graph
    * must have been partitioned using `partitionBy`.
    *
    * @param merge the user-supplied commutative associative function to merge edge attributes
    *              for duplicate edges.
    *
    * @return The resulting graph with a single edge for each (source, dest) vertex pair.
    */
  override def groupEdges(merge: (E, E) => E): PKGraphImpl[V, E] = {
    // Our solution does not support multi-graphs so identical edges are already grouped when adding them
    // for the first time, so this method does nothing
    throw new UnsupportedOperationException
  }

  /**
    * Aggregates values from the neighboring edges and vertices of each vertex. The user-supplied
    * `sendMsg` function is invoked on each edge of the graph, generating 0 or more messages to be
    * sent to either vertex in the edge. The `mergeMsg` function is then used to combine all messages
    * destined to the same vertex.
    *
    * @tparam A the type of message to be sent to each vertex
    *
    * @param sendMsg runs on each edge, sending messages to neighboring vertices using the
    *   [[EdgeContext]].
    * @param mergeMsg used to combine messages from `sendMsg` destined to the same vertex. This
    *   combiner should be commutative and associative.
    * @param tripletFields which fields should be included in the [[EdgeContext]] passed to the
    *   `sendMsg` function. If not all fields are needed, specifying this can improve performance.
    *
    * @example We can use this function to compute the in-degree of each
    * vertex
    * {{{
    * val rawGraph: Graph[_, _] = Graph.textFile("twittergraph")
    * val inDeg: RDD[(VertexId, Int)] =
    *   rawGraph.aggregateMessages[Int](ctx => ctx.sendToDst(1), _ + _)
    * }}}
    *
    * @note By expressing computation at the edge level we achieve
    * maximum parallelism.  This is one of the core functions in the
    * Graph API that enables neighborhood level computation. For
    * example this function can be used to count neighbors satisfying a
    * predicate or implement PageRank.
    *
    */
  override def aggregateMessages[A: ClassTag](
      sendMsg: EdgeContext[V, E, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields = TripletFields.All
  ): VertexRDD[A] = {
    vertices.cache()
    // For each vertex, replicate its attribute only to partitions where it is
    // in the relevant position in an edge.
    replicatedVertexView.upgrade(vertices, tripletFields.useSrc, tripletFields.useDst)

    // Map and combine.
    val preAgg = replicatedVertexView.edges.edgePartitions
      .mapPartitions(_.flatMap { _._2.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields) })
      .setName("PKGraph.aggregateMessages - preAgg")

    // do the final reduction reusing the index map
    vertices.aggregateUsingIndex(preAgg, mergeMsg)
  }

  /**
    * Joins the vertices with entries in the `other` RDD and merges the results using `updateF`.
    * The input table should contain at most one entry for each vertex.  If no entry in `other` is
    * provided for a particular vertex in the graph, the map function receives `None`.
    *
    * @tparam U the type of entry in the table of updates
    * @tparam V2 the new vertex value type
    * @param other the table to join with the vertices in the graph.
    *              The table should contain at most one entry for each vertex.
    * @param updateF the function used to compute the new vertex values.
    *                The map function is invoked for all vertices, even those
    *                that do not have a corresponding entry in the table.
    *
    * @example This function is used to update the vertices with new values based on external data.
    *          For example we could add the out-degree to each vertex record:
    *
    * {{{
    * val rawGraph: Graph[_, _] = Graph.textFile("webgraph")
    * val outDeg: RDD[(VertexId, Int)] = rawGraph.outDegrees
    * val graph = rawGraph.outerJoinVertices(outDeg) {
    *   (vid, data, optDeg) => optDeg.getOrElse(0)
    * }
    * }}}
    *
    * @return new graph with vertices from this graph and the `other` RDD
    */
  override def outerJoinVertices[U: ClassTag, V2: ClassTag](
      other: RDD[(VertexId, U)]
  )(updateF: (VertexId, V, Option[U]) => V2)(implicit eq: V =:= V2 = null): Graph[V2, E] = {
    // The implicit parameter eq will be populated by the compiler if VD and VD2 are equal, and left
    // null if not
    if (eq != null) {
      vertices.cache()
      // updateF preserves type, so we can use incremental replication
      val newVerts = vertices.leftJoin(other)(updateF).cache()
      val changedVerts = vertices.asInstanceOf[PKVertexRDDImpl[V2]].diff(newVerts)
      val newReplicatedVertexView = replicatedVertexView
        .asInstanceOf[PKReplicatedVertexView[V2, E]]
        .updateVertices(changedVerts)
      new PKGraphImpl(newVerts, newReplicatedVertexView)
    } else {
      // updateF does not preserve type, so we must re-replicate all vertices
      val newVerts = vertices.leftJoin(other)(updateF)
      PKGraphImpl(newVerts, replicatedVertexView.edges.asInstanceOf[PKEdgeRDDImpl[V2, E]])
    }
  }
}

object PKGraphImpl {

  /**
    * Create a graph from edges, setting referenced vertices to `defaultVertexAttr`.
    */
  def apply[V: ClassTag, E: ClassTag](
      edges: RDD[Edge[E]],
      defaultVertexAttr: V,
      edgeStorageLevel: StorageLevel,
      vertexStorageLevel: StorageLevel
  ): PKGraphImpl[V, E] = {
    fromEdgeRDD(PKEdgeRDDImpl.fromEdges(edges), defaultVertexAttr, edgeStorageLevel, vertexStorageLevel)
  }

  /**
    * Create a graph from vertices and edges, setting missing vertices to `defaultVertexAttr`.
    */
  def apply[V: ClassTag, E: ClassTag](
      vertices: RDD[(VertexId, V)],
      edges: RDD[Edge[E]],
      defaultVertexAttr: V,
      edgeStorageLevel: StorageLevel,
      vertexStorageLevel: StorageLevel
  ): PKGraphImpl[V, E] = {
    val edgeRDD = PKEdgeRDDImpl
      .fromEdges(edges)(classTag[V], classTag[E])
      .withStorageLevel(edgeStorageLevel)
    val vertexRDD = PKVertexRDDImpl(vertices, edgeRDD, defaultVertexAttr).withStorageLevel(vertexStorageLevel)
    PKGraphImpl(vertexRDD, edgeRDD)
  }

  /**
    * Create a graph from a VertexRDD and an EdgeRDD with arbitrary replicated vertices. The
    * VertexRDD must already be set up for efficient joins with the EdgeRDD by calling
    * `VertexRDD.withEdges` or an appropriate VertexRDD constructor.
    */
  def apply[V: ClassTag, E: ClassTag](vertices: PKVertexRDDImpl[V], edges: PKEdgeRDDImpl[V, E]): PKGraphImpl[V, E] = {
    vertices.cache()

    // Convert the vertex partitions in edges to the correct type
    val newEdges = edges
      .mapEdgePartitions[V, E]((_, part) => part.withoutVertexAttributes[V]())
      .cache()

    fromExistingRDDs(vertices, newEdges)
  }

  /**
    * Create a graph from EdgePartitions, setting referenced vertices to `defaultVertexAttr`.
    */
  def fromEdgePartitions[V: ClassTag, E: ClassTag](
      edgePartitions: RDD[(PartitionID, PKEdgePartition[V, E])],
      defaultVertexAttr: V,
      edgeStorageLevel: StorageLevel,
      vertexStorageLevel: StorageLevel
  ): PKGraphImpl[V, E] = {
    fromEdgeRDD(
      PKEdgeRDDImpl.fromEdgePartitions(edgePartitions),
      defaultVertexAttr,
      edgeStorageLevel,
      vertexStorageLevel
    )
  }

  /**
    * Create a graph from a VertexRDD and an EdgeRDD with the same replicated vertex type as the
    * vertices. The VertexRDD must already be set up for efficient joins with the EdgeRDD by calling
    * `VertexRDD.withEdges` or an appropriate VertexRDD constructor.
    */
  def fromExistingRDDs[V: ClassTag, E: ClassTag](
      vertices: PKVertexRDDImpl[V],
      edges: PKEdgeRDDImpl[V, E]
  ): PKGraphImpl[V, E] = {
    new PKGraphImpl(vertices, new PKReplicatedVertexView(edges))
  }

  /**
    * Create a graph from an EdgeRDD with the correct vertex type, setting missing vertices to
    * `defaultVertexAttr`. The vertices will have the same number of partitions as the EdgeRDD.
    */
  private def fromEdgeRDD[V: ClassTag, E: ClassTag](
      edges: PKEdgeRDDImpl[V, E],
      defaultVertexAttr: V,
      edgeStorageLevel: StorageLevel,
      vertexStorageLevel: StorageLevel
  ): PKGraphImpl[V, E] = {
    val edgesCached = edges.withStorageLevel(edgeStorageLevel).cache()
    val vertices = PKVertexRDDImpl
      .fromEdges(edgesCached, edgesCached.partitions.length, defaultVertexAttr)
      .withStorageLevel(vertexStorageLevel)
    fromExistingRDDs(vertices, edgesCached)
  }
}
