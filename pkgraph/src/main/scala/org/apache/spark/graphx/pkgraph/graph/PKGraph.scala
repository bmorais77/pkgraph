package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.HashPartitioner
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.{EdgeActiveness, VertexRDDImpl}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.language.implicitConversions
import scala.reflect.{ClassTag, classTag}

class PKGraph[V: ClassTag, E: ClassTag] private (
    val k: Int,
    @transient override val vertices: VertexRDD[V],
    @transient val replicatedVertexView: PKReplicatedVertexView[V, E]
) extends Graph[V, E] {
  @transient override val edges: PKEdgeRDD[V, E] = replicatedVertexView.edges

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
    this
  }

  /**
   * Partitions the graph according to a 2D partitioning scheme that arranges the edges of
   * the graph in a grid of roughly equal sized blocks. Requires the number of vertices to be known.
   *
   * @param numPartitions     Number of partitions to create
   * @return graph partitioned with the given [[PartitionStrategy]]
   */
  def partitionByGridStrategy(numPartitions: Int = edges.partitions.length): PKGraph[V, E] = {
    val (matrixSize, _) = vertices.max() { case (a, b) => if(a._1 >= b._1) 1 else -1 }
    partitionBy(new PKGridPartitionStrategy(matrixSize.toInt), numPartitions)
  }

  /**
    * Repartitions the edges in the graph according to the given `partitionStrategy`.
    *
    * @param partitionStrategy the partitioning strategy to use when partitioning the edges in the graph.
    * @return graph partitioned with the given [[PartitionStrategy]]
    */
  override def partitionBy(partitionStrategy: PartitionStrategy): PKGraph[V, E] = {
    partitionBy(partitionStrategy, edges.partitions.length)
  }

  /**
    * Repartitions the edges in the graph according to the given `partitionStrategy`.
    *
    * @param partitionStrategy the partitioning strategy to use when partitioning the edges in the graph.
    * @param numPartitions the number of edge partitions in the new graph.
    * @return graph partitioned with the given [[PartitionStrategy]] and with the given number of partitions
    */
  override def partitionBy(partitionStrategy: PartitionStrategy, numPartitions: Int): PKGraph[V, E] = {
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
          val builder = PKEdgePartitionBuilder[V, E](k)(vTag, eTag)
          iter.foreach { message =>
            val data = message._2
            builder.add(data._1, data._2, data._3)
          }
          val edgePartition = builder.build()
          Iterator((pid, edgePartition))
        },
        preservesPartitioning = true
      )

    val newEdges = edges.withEdgePartitions(partitions).cache()
    val newVertices = PKVertexRDD.withEdges(vertices.asInstanceOf[VertexRDDImpl[V]], newEdges)
    PKGraph.fromExistingRDDs(k, newVertices, newEdges)
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
      val changedVerts = vertices.asInstanceOf[VertexRDD[V2]].diff(newVerts)
      val newReplicatedVertexView = replicatedVertexView
        .asInstanceOf[PKReplicatedVertexView[V2, E]]
        .updateVertices(changedVerts)
      new PKGraph(k, newVerts, newReplicatedVertexView)
    } else {
      // The map does not preserve type, so we must re-replicate all vertices
      PKGraph(k, vertices.mapValues(f), replicatedVertexView.edges.asInstanceOf[PKEdgeRDD[V2, E]])
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
    new PKGraph[V, E2](k, vertices, replicatedVertexView.withEdges(newEdges))
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
    new PKGraph[V, E2](k, vertices, replicatedVertexView.withEdges(newEdges))
  }

  /**
    * Reverses all edges in the graph.  If this graph contains an edge from a to b then the returned
    * graph contains an edge from b to a.
    *
    * @return new graph with reversed edges
    */
  override def reverse: Graph[V, E] = new PKGraph(k, vertices.reverseRoutingTables(), replicatedVertexView.reverse())

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
    val newVerts = vertices.mapVertexPartitions(_.filter(vpred))
    // Filter the triplets. We must always upgrade the triplet view fully because vpred always runs
    // on both src and dst vertices
    replicatedVertexView.upgrade(vertices, includeSrc = true, includeDst = true)
    val newEdges = replicatedVertexView.edges.filter(epred, vpred)
    new PKGraph[V, E](k, newVerts, replicatedVertexView.withEdges(newEdges))
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
    new PKGraph(k, newVerts, replicatedVertexView.withEdges(newEdges))
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
  override def groupEdges(merge: (E, E) => E): PKGraph[V, E] = {
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
    * This variant can take an active set to restrict the computation and is intended for internal
    * use only.
    *
    * @tparam A the type of message to be sent to each vertex
    *
    * @param sendMsg runs on each edge, sending messages to neighboring vertices using the
    *   [[EdgeContext]].
    * @param mergeMsg used to combine messages from `sendMsg` destined to the same vertex. This
    *   combiner should be commutative and associative.
    * @param tripletFields which fields should be included in the [[EdgeContext]] passed to the
    *   `sendMsg` function. If not all fields are needed, specifying this can improve performance.
    * @param activeSetOpt an efficient way to run the aggregation on a subset of the edges if
    *   desired. This is done by specifying a set of "active" vertices and an edge direction. The
    *   `sendMsg` function will then run on only edges connected to active vertices by edges in the
    *   specified direction. If the direction is `In`, `sendMsg` will only be run on edges with
    *   destination in the active set. If the direction is `Out`, `sendMsg` will only be run on edges
    *   originating from vertices in the active set. If the direction is `Either`, `sendMsg` will be
    *   run on edges with *either* vertex in the active set. If the direction is `Both`, `sendMsg`
    *   will be run on edges with *both* vertices in the active set. The active set must have the
    *   same index as the graph's vertices.
    */
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
    val view = activeSetOpt match {
      case Some((activeSet, _)) =>
        replicatedVertexView.withActiveSet(activeSet)
      case None =>
        replicatedVertexView
    }
    val activeDirectionOpt = activeSetOpt.map(_._2)

    // Map and combine.
    val preAgg = view.edges.edgePartitions
      .mapPartitions(_.flatMap {
        case (_, part) =>
          activeDirectionOpt match {
            case Some(EdgeDirection.Both) =>
              part.aggregateMessages(sendMsg, mergeMsg, tripletFields, EdgeActiveness.Both)
            case Some(EdgeDirection.Either) =>
              part.aggregateMessages(sendMsg, mergeMsg, tripletFields, EdgeActiveness.Either)
            case Some(EdgeDirection.Out) =>
              part.aggregateMessages(sendMsg, mergeMsg, tripletFields, EdgeActiveness.SrcOnly)
            case Some(EdgeDirection.In) =>
              part.aggregateMessages(sendMsg, mergeMsg, tripletFields, EdgeActiveness.DstOnly)
            case _ => // None
              part.aggregateMessages(sendMsg, mergeMsg, tripletFields, EdgeActiveness.Neither)
          }
      })
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
      val changedVerts = vertices.asInstanceOf[VertexRDD[V2]].diff(newVerts)
      val newReplicatedVertexView = replicatedVertexView
        .asInstanceOf[PKReplicatedVertexView[V2, E]]
        .updateVertices(changedVerts)
      new PKGraph(k, newVerts, newReplicatedVertexView)
    } else {
      // updateF does not preserve type, so we must re-replicate all vertices
      val newVerts = vertices.leftJoin(other)(updateF)
      PKGraph(k, newVerts, replicatedVertexView.edges.asInstanceOf[PKEdgeRDD[V2, E]])
    }
  }

  /**
    * Performs a full outer join the given vertices RDD.
    *
    * @param other Vertex RDD
    * @return new Graph with added vertices
    */
  def fullOuterJoinVertices(other: RDD[(VertexId, V)]): Graph[V, E] = {
    val partitions = vertices
      .fullOuterJoin(other)
      .map { v =>
        v._2._2 match {
          case Some(attr) => (v._1, attr)
          case None =>
            v._2._1 match {
              case Some(attr) => (v._1, attr)
              case None       => throw new Exception(s"no attribute for vertex ${v._1}")
            }
        }
      }

    val newVertices = PKVertexRDD(partitions, edges, null.asInstanceOf[V])
    val changedVertices = vertices.diff(newVertices)
    val newReplicatedVertexView = replicatedVertexView.updateVertices(changedVertices)
    new PKGraph(k, newVertices, newReplicatedVertexView)
  }

  /**
    * Performs a full outer join the given edge RDD.
    * Note: The operation is optimized if the RDD is already partitioned as the existing edge
    * RDD in this graph, if not a default partition strategy is used.
    *
    * @param other Edge RDD
    * @return new Graph with added edges
    */
  def fullOuterJoinEdges(other: RDD[Edge[E]]): Graph[V, E] = {
    fullOuterJoinEdges(other, PartitionStrategy.EdgePartition2D, edges.partitions.length)
  }

  /**
    * Performs a full outer join the given edge RDD.
    *
    * @param other Edge RDD
    * @param strategy [[PartitionStrategy]] to partition 'other' RDD with
    * @return new Graph with added edges
    */
  def fullOuterJoinEdges(other: RDD[Edge[E]], strategy: PartitionStrategy): Graph[V, E] = {
    fullOuterJoinEdges(other, strategy, edges.partitions.length)
  }

  /**
    * Performs a full outer join the given edge RDD.
    * If the 'other' RDD does not have the same partition has the current edge RDD in this graph,
    * it will be partitioned using the given [[PartitionStrategy]] into the given number of partitions.
    *
    * @param other Edge RDD
    * @param strategy [[PartitionStrategy]] to partition 'other' RDD with
    * @param numPartitions Number of partitions to partition 'other' RDD in
    * @return new Graph with added edges
    */
  def fullOuterJoinEdges(other: RDD[Edge[E]], strategy: PartitionStrategy, numPartitions: Int): Graph[V, E] = {
    // Test if the other edge is already partitioned
    val partitions = other match {
      case other if edges.partitioner == other.partitioner =>
        edges.edgePartitions.zipPartitions(other, preservesPartitioning = true) { (partIter, edges) =>
          partIter.map(v => (v._1, v._2.addEdges(edges)))
        }
      case _ =>
        other
          .map(e => (strategy.getPartition(e.srcId, e.dstId, numPartitions), e))
          .partitionBy(edges.partitioner.get)
          .zipPartitions(edges.edgePartitions, preservesPartitioning = true) { (edges, partIter) =>
            partIter.map(e => (e._1, e._2.addEdges(edges.map(_._2))))
          }
    }

    val newEdges = edges.withEdgePartitions(partitions)
    new PKGraph[V, E](k, vertices, replicatedVertexView.withEdges(newEdges))
  }

  override val ops: GraphOps[V, E] = new PKGraphOps(this)
}

object PKGraph {

  /**
    * Construct a graph from a collection of vertices and
    * edges with attributes.  Duplicate vertices are picked arbitrarily and
    * vertices found in the edge collection but not in the input
    * vertices are assigned the default attribute.
    *
    * @tparam V the vertex attribute type
    * @tparam E the edge attribute type
    * @param k K2Tree value
    * @param vertices the "set" of vertices and their attributes
    * @param edges the collection of edges in the graph
    * @param defaultVertexAttr the default vertex attribute to use for vertices that are
    *                          mentioned in edges but not in vertices
    * @param edgeStorageLevel the desired storage level at which to cache the edges if necessary
    * @param vertexStorageLevel the desired storage level at which to cache the vertices if necessary
    */
  def apply[V: ClassTag, E: ClassTag](
      k: Int,
      vertices: RDD[(VertexId, V)],
      edges: RDD[Edge[E]],
      defaultVertexAttr: V = null.asInstanceOf[V],
      edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
  ): PKGraph[V, E] = {
    val edgeRDD = PKEdgeRDD
      .fromEdges(k, edges)(classTag[V], classTag[E])
      .withTargetStorageLevel(edgeStorageLevel)
    val vertexRDD = PKVertexRDD(vertices, edgeRDD, defaultVertexAttr).withTargetStorageLevel(vertexStorageLevel)
    PKGraph(k, vertexRDD, edgeRDD)
  }

  /**
    * Create a graph from a VertexRDD and an EdgeRDD with arbitrary replicated vertices. The
    * VertexRDD must already be set up for efficient joins with the EdgeRDD by calling
    * `VertexRDD.withEdges` or an appropriate VertexRDD constructor.
    *
    * @param k K2Tree value
    * @param vertices Vertices RDD
    * @param edges Edges RDD
    * @return new [[PKGraph]] with given RDDs
    */
  def apply[V: ClassTag, E: ClassTag](k: Int, vertices: VertexRDD[V], edges: PKEdgeRDD[V, E]): PKGraph[V, E] = {
    vertices.cache()

    // Convert the vertex partitions in edges to the correct type
    val newEdges = edges
      .mapEdgePartitions[V, E]((_, part) => part.withoutVertexAttributes[V]())
      .cache()

    fromExistingRDDs(k, vertices, newEdges)
  }

  /**
    * Construct a graph from a collection of edges.
    *
    * @param k K2Tree value
    * @param edges the RDD containing the set of edges in the graph
    * @param defaultVertexAttr the default vertex attribute to use for each vertex
    * @param edgeStorageLevel the desired storage level at which to cache the edges if necessary
    * @param vertexStorageLevel the desired storage level at which to cache the vertices if necessary
    *
    * @return a graph with edge attributes described by `edges` and vertices
    *         given by all vertices in `edges` with value `defaultValue`
    */
  def fromEdges[V: ClassTag, E: ClassTag](
      k: Int,
      edges: RDD[Edge[E]],
      defaultVertexAttr: V,
      edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
  ): PKGraph[V, E] = {
    fromEdgeRDD(k, PKEdgeRDD.fromEdges(k, edges), defaultVertexAttr, edgeStorageLevel, vertexStorageLevel)
  }

  /**
    * Construct a graph from a collection of edges encoded as vertex id pairs.
    *
    * @param k K2Tree value
    * @param rawEdges a collection of edges in (src, dst) form
    * @param defaultValue the vertex attributes with which to create vertices referenced by the edges
    * @param edgeStorageLevel the desired storage level at which to cache the edges if necessary
    * @param vertexStorageLevel the desired storage level at which to cache the vertices if necessary
    *
    * @return a graph with edge attributes containing either the count of duplicate edges or 1
    * (if `uniqueEdges` is `None`) and vertex attributes containing the total degree of each vertex.
    */
  def fromEdgeTuples[V: ClassTag, E: ClassTag](
      k: Int,
      rawEdges: RDD[(VertexId, VertexId)],
      defaultValue: V,
      edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
  ): PKGraph[V, Int] = {
    val edges = rawEdges.map(p => Edge(p._1, p._2, 1))
    fromEdges(k, edges, defaultValue, edgeStorageLevel, vertexStorageLevel)
  }

  /**
    * Create a graph from an EdgeRDD with the correct vertex type, setting missing vertices to
    * `defaultVertexAttr`. The vertices will have the same number of partitions as the EdgeRDD.
    */
  private def fromEdgeRDD[V: ClassTag, E: ClassTag](
      k: Int,
      edges: PKEdgeRDD[V, E],
      defaultVertexAttr: V,
      edgeStorageLevel: StorageLevel,
      vertexStorageLevel: StorageLevel
  ): PKGraph[V, E] = {
    val edgesCached = edges.withTargetStorageLevel(edgeStorageLevel).cache()
    val vertices = PKVertexRDD
      .fromEdges(edgesCached, edgesCached.partitions.length, defaultVertexAttr)
      .withTargetStorageLevel(vertexStorageLevel)
    fromExistingRDDs(k, vertices, edgesCached)
  }

  /**
    * Create a graph from a VertexRDD and an EdgeRDD with the same replicated vertex type as the
    * vertices. The VertexRDD must already be set up for efficient joins with the EdgeRDD by calling
    * `VertexRDD.withEdges` or an appropriate VertexRDD constructor.
    */
  private def fromExistingRDDs[V: ClassTag, E: ClassTag](
      k: Int,
      vertices: VertexRDD[V],
      edges: PKEdgeRDD[V, E]
  ): PKGraph[V, E] = {
    new PKGraph(k, vertices, new PKReplicatedVertexView(edges))
  }

  /**
    * Implicitly extracts the [[GraphOps]] member from a graph.
    *
    * To improve modularity the Graph type only contains a small set of basic operations.
    * All the convenience operations are defined in the [[GraphOps]] class which may be
    * shared across multiple graph implementations.
    */
  implicit def graphToGraphOps[V: ClassTag, E: ClassTag](g: PKGraph[V, E]): GraphOps[V, E] = g.ops
}
