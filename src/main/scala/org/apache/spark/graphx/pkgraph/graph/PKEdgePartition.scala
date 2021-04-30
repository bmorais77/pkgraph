package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.EdgeActiveness
import org.apache.spark.graphx.pkgraph.compression.{K2Tree, K2TreeBuilder, K2TreeIndex}
import org.apache.spark.graphx.pkgraph.util.mathx
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.util.collection.PrimitiveVector

import scala.reflect.ClassTag

// TODO: activeSet - should use a HashSet because the min offset is used and can lead to very large indices
// TODO: innerjoin - Compare offsets and size to determine if there is an intersection at all between the partitions
// TODO: innerjoin - For partitions with same offsets and same sizes perform a K²-Tree inner join operation
// TODO: innerjoin - For partitions with different offsets/sizes we could grow one of the trees to now have the same offset/size and perform a K²-Tree inner join operation

/**
 * Implementations:
 * Constant Access Index (constantIndex) - Store a map from edge index to its position in the edge array
 * Linear Access Index (linearIndex)     - Store a bitset with bits set to 1 for the edge indices that exist
 * Storing vertex indices (vertexIndex)  - Store a bitset for source and one for destination vertices to keep track of the vertices that exist
 */

/**
  *
  * @param vertexAttrs Maps vertex global identifier to their attribute
  * @param edgeAttrs Stores edge attributes and their corresponding index
  * @param tree K2Tree representing edges
  * @param srcOffset Source identifier offset
  * @param dstOffset Destination identifier offset
  * @param activeSet BitSet to keep track of active vertices
  * @tparam V Vertex attribute type
  * @tparam E Edge attribute type
  */
private[graph] class PKEdgePartition[V: ClassTag, E: ClassTag](
    val vertexAttrs: GraphXPrimitiveKeyOpenHashMap[VertexId, V],
    val edgeAttrs: EdgeAttributesMap[E],
    val tree: K2Tree,
    val srcOffset: Long,
    val dstOffset: Long,
    val activeSet: Option[VertexSet]
) {

  /**
    * Return a new edge partition without any locally cached vertex attributes.
    *
    * @tparam V2 new type of vertices
    * @return edge partition without cached vertices
    */
  def withoutVertexAttributes[V2: ClassTag](): PKEdgePartition[V2, E] = {
    val newVertexAttrs = new GraphXPrimitiveKeyOpenHashMap[VertexId, V2](vertexAttrs.keySet.capacity)
    new PKEdgePartition(newVertexAttrs, edgeAttrs, tree, srcOffset, dstOffset, activeSet)
  }

  /**
    * Return a new [[PKEdgePartition]] with the specified active set, provided as an iterator.
    *
    * @param iter Active set as iterator
    * @return [[PKEdgePartition]] with given active set
    */
  def withActiveSet(iter: Iterator[VertexId]): PKEdgePartition[V, E] = {
    val activeSet = new VertexSet
    while (iter.hasNext) {
      activeSet.add(iter.next())
    }
    new PKEdgePartition(vertexAttrs, edgeAttrs, tree, srcOffset, dstOffset, Some(activeSet))
  }

  /**
    * Look up vid in activeSet, throwing an exception if it is None.
    *
    * @param vid Vertex identifier
    * @return true if vertex is active, false otherwise
    */
  def isActive(vid: VertexId): Boolean = activeSet.get.contains(vid)

  /**
    * The number of active vertices.
    *
    * @return number of active vertices
    */
  def numActives: Int = activeSet.map(_.size).getOrElse(0)

  /**
    * Gives the number of vertices in this partition (estimate).
    *
    * @return number of vertices
    */
  def numVertices: Int = edgeAttrs.size / 2

  /**
    * Returns a new [[PKEdgePartition]] with the given edges added.
    *
    * @param edges Edges to add
    * @return new partition with edges added
    */
  def addEdges(edges: Iterator[Edge[E]]): PKEdgePartition[V, E] = {
    val (newTree, newEdges, newSrcOffset, newDstOffset) = preprocessNewEdges(edges)
    val builder = PKExistingEdgePartitionBuilder[V, E](this, newTree.toBuilder, edgeAttrs, newSrcOffset, newDstOffset)

    for (edge <- newEdges) {
      builder.addEdge(edge.srcId, edge.dstId, edge.attr)
    }

    builder.build
  }

  /**
    * Returns new [[PKEdgePartition]] with the given edges removed.
    *
    * @param edges Edges to remove
    * @return new partition with edges removed
    */
  def removeEdges(edges: Iterator[(VertexId, VertexId)]): PKEdgePartition[V, E] = {
    val builder = PKExistingEdgePartitionBuilder[V, E](this, tree.toBuilder)

    // Add existing edges to build indices
    for (edge <- iterator) {
      builder.addEdge(edge.srcId, edge.dstId, edge.attr)
    }

    for ((src, dst) <- edges) {
      builder.removeEdge(src, dst)
    }

    builder.build
  }

  /**
    * Updates the vertex attributes in this partition with the ones in the given iterator.
    *
    * @param iter Iterator with vertex attributes
    * @return new [[PKEdgePartition]] with updated vertices
    */
  def updateVertices(iter: Iterator[(VertexId, V)]): PKEdgePartition[V, E] = {
    // Copy existing vertex attributes
    val newVertexAttrs = new GraphXPrimitiveKeyOpenHashMap[VertexId, V](vertexAttrs.keySet.capacity)
    for ((id, attr) <- vertexAttrs) {
      newVertexAttrs(id) = attr
    }

    // Update with new attributes
    while (iter.hasNext) {
      val (id, attr) = iter.next()
      newVertexAttrs(id) = attr
    }
    new PKEdgePartition(newVertexAttrs, edgeAttrs, tree, srcOffset, dstOffset, activeSet)
  }

  /**
    * Reverse all the edges in this partition.
    *
    * @return a new edge partition with all edges reversed.
    */
  def reverse: PKEdgePartition[V, E] = {
    val newTreeBuilder = K2TreeBuilder(tree.k, tree.size)
    val builder = PKExistingEdgePartitionBuilder[V, E](this, newTreeBuilder)

    // Traverse all edges and reverse their source/destination vertices
    for (edge <- iterator) {
      builder.addEdge(edge.dstId, edge.srcId, edge.attr)
    }

    builder.build
  }

  /**
    * Construct a new edge partition by applying the function f to all
    * edges in this partition.
    *
    * Be careful not to keep references to the objects passed to `f`.
    * To improve GC performance the same object is re-used for each call.
    *
    * @param f a function from an edge to a new attribute
    * @tparam E2 the type of the new attribute
    * @return a new edge partition with the result of the function `f` applied to each edge
    */
  def map[E2: ClassTag](f: Edge[E] => E2): PKEdgePartition[V, E2] = {
    val newData = new Array[E2](edgeAttrs.values.length)
    var i = 0

    for (edge <- iterator) {
      newData(i) = f(edge)
      i += 1
    }

    withEdgeAttrs(newData)
  }

  /**
    * Construct a new edge partition by using the edge attributes
    * contained in the iterator.
    *
    * @note The input iterator should return edge attributes in the
    * order of the edges returned by `EdgePartition.iterator` and
    * should return attributes equal to the number of edges.
    *
    * @param iter an iterator for the new attribute values
    * @tparam E2 the type of the new attribute
    * @return a new edge partition with the attribute values replaced
    */
  def map[E2: ClassTag](iter: Iterator[E2]): PKEdgePartition[V, E2] = {
    // Faster than iter.toArray, because the expected size is known.
    val newData = new Array[E2](edgeAttrs.values.length)
    var i = 0
    while (iter.hasNext) {
      newData(i) = iter.next()
      i += 1
    }
    assert(newData.length == i)
    withEdgeAttrs(newData)
  }

  /**
    * Construct a new edge partition containing only the edges matching `epred` and where both
    * vertices match `vpred`.
    *
    * @param epred Edge predicate
    * @param vpred Vertex predicate
    * @return edge partition containing only edges and vertices that match the predicate
    */
  def filter(epred: EdgeTriplet[V, E] => Boolean, vpred: (VertexId, V) => Boolean): PKEdgePartition[V, E] = {
    val newTreeBuilder = K2TreeBuilder(tree.k, tree.size)
    val builder = PKExistingEdgePartitionBuilder[V, E](this, newTreeBuilder)
    for (edge <- iterator) {
      // The user sees the EdgeTriplet, so we can't reuse it and must create one per edge.
      val triplet = new EdgeTriplet[V, E]
      triplet.srcId = edge.srcId
      triplet.dstId = edge.dstId
      triplet.srcAttr = vertexAttrs(edge.srcId)
      triplet.dstAttr = vertexAttrs(edge.dstId)
      triplet.attr = edge.attr

      if (vpred(triplet.srcId, triplet.srcAttr) && vpred(triplet.dstId, triplet.dstAttr) && epred(triplet)) {
        builder.addEdge(triplet.srcId, triplet.dstId, triplet.attr)
      }
    }
    builder.build
  }

  /**
    * Apply the function f to all edges in this partition.
    *
    * @param f an external state mutating user defined function.
    */
  def foreach(f: Edge[E] => Unit): Unit = {
    iterator.foreach(f)
  }

  /**
    * Apply `f` to all edges present in both `this` and `other` and return a new `PKEdgePartition`
    * containing the resulting edges.
    *
    * If there are multiple edges with the same src and dst in `this`, `f` will be invoked once for
    * each edge, but each time it may be invoked on any corresponding edge in `other`.
    *
    * If there are multiple edges with the same src and dst in `other`, `f` will only be invoked
    * once.
    *
    * @param other Other edge partition to join with
    * @param f Function to compute merged edge
    * @return new partition with resulting edges
    */
  def innerJoin[E2: ClassTag, E3: ClassTag](
      other: PKEdgePartition[_, E2]
  )(f: (VertexId, VertexId, E, E2) => E3): PKEdgePartition[V, E3] = {
    val newTreeBuilder = K2TreeBuilder(tree.k, tree.size)
    val builder = PKExistingEdgePartitionBuilder[V, E3](this, newTreeBuilder)
    val comparator = new PKEdgeComparator[E, E2](this, other)

    val it1 = iterator
    val it2 = other.iterator

    var edge2: Edge[E2] = Edge(-1, -1, null.asInstanceOf[E2])
    while (it1.hasNext && it2.hasNext) {
      val edge1 = it1.next()

      while (it2.hasNext && comparator.compare(edge1, edge2) > 0) {
        edge2 = it2.next()
      }

      if (edge1.srcId == edge2.srcId && edge1.dstId == edge2.dstId) {
        builder.addEdge(edge1.srcId, edge1.dstId, f(edge1.srcId, edge1.dstId, edge1.attr, edge2.attr))
      }
    }

    builder.build
  }

  /**
    * The number of edges in this partition
    *
    * @return size of the partition
    */
  val size: Int = edgeAttrs.values.length

  /**
    * Get an iterator over the edges in this partition.
    * Note: This iterator can be slightly faster than the [[K2TreeIterator]] because it counts the number of edges,
    * avoiding having to search the entire tree.
    *
    * Be careful not to keep references to the objects from this iterator.
    * To improve GC performance the same object is re-used in `next()`.
    *
    * @return an iterator over edges in the partition
    */
  def iterator: Iterator[Edge[E]] =
    new Iterator[Edge[E]] {
      private val iterator = tree.iterator
      private val edge = new Edge[E]
      private var pos = 0

      override def hasNext: Boolean = pos < edgeAttrs.size && iterator.hasNext

      override def next(): Edge[E] = {
        val nextEdge = iterator.next()
        edge.srcId = nextEdge.line + srcOffset
        edge.dstId = nextEdge.col + dstOffset
        edge.attr = edgeAttrs.values(pos)
        pos += 1
        edge
      }
    }

  /**
    * Get an iterator over the edge triplets in this partition.
    * Note: This iterator can be slightly faster than the [[K2TreeIterator]] because it counts the number of edges,
    * avoiding having to search the entire tree.
    *
    * It is safe to keep references to the objects from this iterator.
    *
    * @param includeSrc Include source vertex attributes
    * @param includeDst Include destination vertex
    * @return edge triplet iterator
    */
  def tripletIterator(includeSrc: Boolean = true, includeDst: Boolean = true): Iterator[EdgeTriplet[V, E]] =
    new Iterator[EdgeTriplet[V, E]] {
      private val iterator = tree.iterator
      private var pos = 0

      override def hasNext: Boolean = pos < edgeAttrs.size && iterator.hasNext

      override def next(): EdgeTriplet[V, E] = {
        val triplet = new EdgeTriplet[V, E]

        val edge = iterator.next()
        triplet.srcId = edge.line + srcOffset
        triplet.dstId = edge.col + dstOffset
        triplet.attr = edgeAttrs.values(pos)

        if (includeSrc) {
          triplet.srcAttr = vertexAttrs(edge.line + srcOffset)
        }

        if (includeDst) {
          triplet.dstAttr = vertexAttrs(edge.col + dstOffset)
        }

        pos += 1
        triplet
      }
    }

  /**
    * Send messages along edges and aggregate them at the receiving vertices. Implemented by scanning
    * all edges sequentially.
    *
    * @param sendMsg generates messages to neighboring vertices of an edge
    * @param mergeMsg the combiner applied to messages destined to the same vertex
    * @param tripletFields which triplet fields `sendMsg` uses
    *
    * @return iterator aggregated messages keyed by the receiving vertex id
    */
  def aggregateMessagesEdgeScan[A: ClassTag](
      sendMsg: EdgeContext[V, E, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields,
      activeness: EdgeActiveness
  ): Iterator[(VertexId, A)] = {
    val ctx = PKAggregatingEdgeContext[V, E, A](mergeMsg)
    for (edge <- iterator) {
      if (isEdgeActive(edge.srcId, edge.dstId, activeness)) {
        val srcAttr = if (tripletFields.useSrc) vertexAttrs(edge.srcId) else null.asInstanceOf[V]
        val dstAttr = if (tripletFields.useDst) vertexAttrs(edge.dstId) else null.asInstanceOf[V]
        ctx.set(edge.srcId, edge.dstId, srcAttr, dstAttr, edge.attr)
        sendMsg(ctx)
      }
    }
    ctx.iterator
  }

  /**
    * Send messages along edges and aggregate them at the receiving vertices. Implemented by
    * filtering the source vertex index, then scanning each edge cluster.
    *
    * @param sendMsg generates messages to neighboring vertices of an edge
    * @param mergeMsg the combiner applied to messages destined to the same vertex
    * @param tripletFields which triplet fields `sendMsg` uses
    * @param activeness criteria for filtering edges based on activeness
    *
    * @return iterator aggregated messages keyed by the receiving vertex id
    */
  def aggregateMessagesSrcIndexScan[A: ClassTag](
      sendMsg: EdgeContext[V, E, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields,
      activeness: EdgeActiveness
  ): Iterator[(VertexId, A)] = {
    val ctx = PKAggregatingEdgeContext[V, E, A](mergeMsg)
    for (src <- 0 until tree.size) {
      val globalSrc: VertexId = src + srcOffset
      if (isSrcVertexActive(globalSrc, activeness)) {
        val srcAttr = if (tripletFields.useSrc) vertexAttrs(globalSrc) else null.asInstanceOf[V]
        val it = edgeAttrs.iterable
        tree.iterateDirectNeighbors(src) { dst =>
          val globalDst: VertexId = dst + dstOffset
          if (isEdgeActive(globalSrc, globalDst, activeness)) {
            val dstAttr = if (tripletFields.useDst) vertexAttrs(globalDst) else null.asInstanceOf[V]
            val index = K2TreeIndex.fromEdge(tree.k, tree.height, src, dst)
            val attr = it.nextAttribute(index)
            ctx.set(globalSrc, globalDst, srcAttr, dstAttr, attr)
            sendMsg(ctx)
          }
        }
      }
    }
    ctx.iterator
  }

  /**
    * Send messages along edges and aggregate them at the receiving vertices. Implemented by
    * filtering the destination vertex index, then scanning each edge cluster.
    *
    * @param sendMsg generates messages to neighboring vertices of an edge
    * @param mergeMsg the combiner applied to messages destined to the same vertex
    * @param tripletFields which triplet fields `sendMsg` uses
    * @param activeness criteria for filtering edges based on activeness
    *
    * @return iterator aggregated messages keyed by the receiving vertex id
    */
  def aggregateMessagesDstIndexScan[A: ClassTag](
      sendMsg: EdgeContext[V, E, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields,
      activeness: EdgeActiveness
  ): Iterator[(VertexId, A)] = {
    val ctx = PKAggregatingEdgeContext[V, E, A](mergeMsg)
    var pos = 0

    for (dst <- 0 until tree.size) {
      val globalDst: VertexId = dst + dstOffset
      if (isDstVertexActive(globalDst, activeness)) {
        val dstAttr = if (tripletFields.useDst) vertexAttrs(globalDst) else null.asInstanceOf[V]
        val it = edgeAttrs.iterable
        tree.iterateReverseNeighbors(dst) { src =>
          val globalSrc: VertexId = src + srcOffset
          if (isEdgeActive(globalSrc, globalDst, activeness)) {
            val srcAttr = if (tripletFields.useSrc) vertexAttrs(globalSrc) else null.asInstanceOf[V]
            val index = K2TreeIndex.fromEdge(tree.k, tree.height, src, dst)
            val attr = it.nextAttribute(index)
            ctx.set(globalSrc, globalDst, srcAttr, dstAttr, attr)
            sendMsg(ctx)
          }
        }
      }
    }
    ctx.iterator
  }

  /**
    * Return a new edge partition with the specified edge data.
    * Note: It is assumed that the order of the edge attribute was preserved.
    *
    * @param attrs Edge attributes
    * @tparam E2 new type of edges
    * @return edge partition with given edge data
    */
  private def withEdgeAttrs[E2: ClassTag](attrs: Array[E2]): PKEdgePartition[V, E2] = {
    val newEdgeAttrs = new EdgeAttributesMap[E2](edgeAttrs.indices, attrs)
    new PKEdgePartition(vertexAttrs, newEdgeAttrs, tree, srcOffset, dstOffset, activeSet)
  }

  /**
    * Checks if the given source vertex is active according to the [[EdgeActiveness]] parameter.
    *
    * @param clusterId Source cluster identifier
    * @param activeness [[EdgeActiveness]] parameter
    * @return true if cluster is active, false otherwise
    */
  private def isSrcVertexActive(clusterId: VertexId, activeness: EdgeActiveness): Boolean = {
    activeness match {
      case EdgeActiveness.SrcOnly => isActive(clusterId)
      case EdgeActiveness.DstOnly => true
      case EdgeActiveness.Both    => isActive(clusterId)
      case EdgeActiveness.Either  => true
      case EdgeActiveness.Neither => true
    }
  }

  /**
    * Checks if the given destination vertex is active according to the [[EdgeActiveness]] parameter.
    *
    * @param clusterId Destination cluster identifier
    * @param activeness [[EdgeActiveness]] parameter
    * @return true if cluster is active, false otherwise
    */
  private def isDstVertexActive(clusterId: VertexId, activeness: EdgeActiveness): Boolean = {
    activeness match {
      case EdgeActiveness.SrcOnly => true
      case EdgeActiveness.DstOnly => isActive(clusterId)
      case EdgeActiveness.Both    => isActive(clusterId)
      case EdgeActiveness.Either  => true
      case EdgeActiveness.Neither => true
    }
  }

  /**
    * Checks if the given edge is active according to the [[EdgeActiveness]] parameter.
    *
    * @param srcId Source vertex identifier
    * @param dstId Destination vertex identifier
    * @param activeness [[EdgeActiveness]] parameter
    * @return true if edge is active, false otherwise
    */
  private def isEdgeActive(srcId: VertexId, dstId: VertexId, activeness: EdgeActiveness): Boolean = {
    activeness match {
      case EdgeActiveness.SrcOnly => isActive(srcId)
      case EdgeActiveness.DstOnly => isActive(dstId)
      case EdgeActiveness.Both    => isActive(srcId) && isActive(dstId)
      case EdgeActiveness.Either  => isActive(srcId) || isActive(dstId)
      case EdgeActiveness.Neither => true
    }
  }

  /**
    * Pre-process new edges to add, to check if the existing K²-Tree needs to grow and
    * calculate new src/dst offsets.
    *
    * @param edges New edges to be added to K²-Tree
    * @return [[Tuple4]] with K²-Tree, new edges, source offset and destination offset
    */
  private def preprocessNewEdges(edges: Iterator[Edge[E]]): (K2Tree, Array[Edge[E]], Long, Long) = {
    val k2 = tree.k * tree.k
    var newSrcOffset = srcOffset
    var newDstOffset = dstOffset
    var newSrcEnd = srcOffset + tree.size
    var newDstEnd = dstOffset + tree.size

    // Traverse edges to check if there are any behind the virtual origin (lineOffset, colOffset) or
    // after the size of the current matrix
    val newEdges = new PrimitiveVector[Edge[E]]
    for (edge <- edges) {
      newSrcOffset = math.min(edge.srcId, newSrcOffset)
      newDstOffset = math.min(edge.dstId, newDstOffset)
      newSrcEnd = math.max(edge.srcId, newSrcEnd)
      newDstEnd = math.max(edge.dstId, newDstEnd)
      newEdges += edge
    }

    // Align offsets to multiple of K2
    newSrcOffset = if (newSrcOffset % k2 == 0) newSrcOffset else newSrcOffset / k2
    newDstOffset = if (newDstOffset % k2 == 0) newDstOffset else newDstOffset / k2

    val newTree = growK2Tree(newSrcOffset, newSrcEnd, newDstOffset, newDstEnd)
    (newTree, newEdges.trim().array, newSrcOffset, newDstOffset)
  }

  /**
    * Creates a new K²-Tree by growing the one in this partition.
    * The adjacency matrix of the K²-Tree either grows to the right and down if the new size exceeds the
    * original size of the matrix, or to the left and up if the new offset is 'behind' the original offset.
    *
    * @param newSrcOffset New source offset
    * @param newSrcEnd New source end
    * @param newDstOffset New destination offset
    * @param newDstEnd New destination end
    * @return K²-Tree with new size
    */
  private def growK2Tree(newSrcOffset: Long, newSrcEnd: Long, newDstOffset: Long, newDstEnd: Long): K2Tree = {
    val behindOrigin = newSrcOffset < srcOffset || newDstOffset < dstOffset
    val afterEnd = newSrcEnd > srcOffset + tree.size || newDstEnd > dstOffset + tree.size

    // Check if new edges are outside the current adjacency matrix
    if (behindOrigin || afterEnd) {
      var newSize = math.max(newSrcEnd - newSrcOffset, newDstEnd - newDstOffset).toInt

      // Size is not a power of K, so we need to find the nearest power
      if (newSize % tree.k != 0) {
        val exp = math.ceil(mathx.log(tree.k, newSize))
        newSize = math.pow(tree.k, exp).toInt
      }

      tree.grow(newSize, behindOrigin)
    } else {
      tree
    }
  }
}
