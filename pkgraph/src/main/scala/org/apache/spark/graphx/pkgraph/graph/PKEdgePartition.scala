package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.EdgeActiveness
import org.apache.spark.graphx.pkgraph.compression.K2Tree
import org.apache.spark.graphx.pkgraph.util.collection.Bitset
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.util.collection.OpenHashSet

import scala.reflect.ClassTag

/**
  * Partitioned K²-Tree edge partition.
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
private[pkgraph] class PKEdgePartition[
    @specialized(Char, Int, Boolean, Byte, Long, Float, Double) V: ClassTag,
    E: ClassTag
](
    val vertexAttrs: Array[V],
    val global2local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int],
    val edgeAttrs: Array[E],
    val tree: K2Tree,
    val srcOffset: Long,
    val dstOffset: Long,
    val activeSet: Option[VertexSet]
) extends Serializable {

  /**
    * Return a new edge partition without any locally cached vertex attributes.
    *
    * @tparam V2 new type of vertices
    * @return edge partition without cached vertices
    */
  def withoutVertexAttributes[V2: ClassTag](): PKEdgePartition[V2, E] = {
    val newVertexAttrs = new Array[V2](vertexAttrs.length)
    new PKEdgePartition(
      newVertexAttrs,
      global2local,
      edgeAttrs,
      tree,
      srcOffset,
      dstOffset,
      activeSet
    )
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
    new PKEdgePartition(
      vertexAttrs,
      global2local,
      edgeAttrs,
      tree,
      srcOffset,
      dstOffset,
      Some(activeSet)
    )
  }

  /**
    * Look up vid in activeSet, throwing an exception if it is None.
    *
    * @param vid Vertex identifier
    * @return true if vertex is active, false otherwise
    */
  def isActive(vid: VertexId): Boolean = activeSet.get.contains(vid)

  /**
    * Get the vertex attribute of the vertex with the given global identifier.
    *
    * @param vid Vertex identifier
    * @return vertex attribute
    */
  def vertexAttribute(vid: VertexId): V = vertexAttrs(global2local(vid))

  /**
    * Returns a new [[PKEdgePartition]] with the given edges added.
    *
    * @param edges Edges to add
    * @return new partition with edges added
    */
  def addEdges(edges: Iterator[Edge[E]]): PKEdgePartition[V, E] = {
    val builder = PKEdgePartitionBuilder[V, E](tree.k, tree.size)

    // Add existing edges
    for (edge <- iterator) {
      builder.add(edge.srcId, edge.dstId, edge.attr)
    }

    // Add new edges
    for (edge <- edges) {
      builder.add(edge.srcId, edge.dstId, edge.attr)
    }

    builder.build()
  }

  /**
    * Returns new [[PKEdgePartition]] with the given edges removed.
    *
    * @param edges Edges to remove
    * @return new partition with edges removed
    */
  def removeEdges(edges: Iterator[(VertexId, VertexId)]): PKEdgePartition[V, E] = {
    val removedEdges = new OpenHashSet[Int]()

    // Build hashset containing edges to be removed
    for ((src, dst) <- edges) {
      val line = (src - srcOffset).toInt
      val col = (dst - dstOffset).toInt
      removedEdges.add(line * tree.size + col)
    }

    val builder = PKExistingEdgePartitionBuilder[V, E](this)

    // Add existing edges expect from removed vertices
    for (edge <- iterator) {
      val line = (edge.srcId - srcOffset).toInt
      val col = (edge.dstId - dstOffset).toInt

      if (!removedEdges.contains(line * tree.size + col)) {
        builder.addEdge(edge.srcId, edge.dstId, edge.attr)
      }
    }

    builder.build()
  }

  /**
    * Updates the vertex attributes in this partition with the ones in the given iterator.
    *
    * @param iter Iterator with vertex attributes
    * @return new [[PKEdgePartition]] with updated vertices
    */
  def updateVertices(iter: Iterator[(VertexId, V)]): PKEdgePartition[V, E] = {
    val newVertexAttrs = new Array[V](vertexAttrs.length)
    System.arraycopy(vertexAttrs, 0, newVertexAttrs, 0, vertexAttrs.length)
    while (iter.hasNext) {
      val (id, attr) = iter.next()
      newVertexAttrs(global2local(id)) = attr
    }

    new PKEdgePartition(
      newVertexAttrs,
      global2local,
      edgeAttrs,
      tree,
      srcOffset,
      dstOffset,
      activeSet
    )
  }

  /**
    * Reverse all the edges in this partition.
    *
    * @return a new edge partition with all edges reversed.
    */
  def reverse: PKEdgePartition[V, E] = {
    // We can't use an existing partition builder since the edges won't added in their correct order
    val builder = PKEdgePartitionBuilder[V, E](tree.k, tree.size)

    // Traverse all edges and reverse their source/destination vertices
    for (edge <- iterator) {
      builder.add(edge.dstId, edge.srcId, edge.attr)
    }

    builder.build()
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
    val newData = new Array[E2](edgeAttrs.length)
    var i = 0

    for (edge <- iterator) {
      newData(i) = f(edge)
      i += 1
    }

    assert(newData.length == i, s"new data length '$i' (expected: ${newData.length})")
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
    val newData = new Array[E2](edgeAttrs.length)
    var i = 0
    while (iter.hasNext) {
      newData(i) = iter.next()
      i += 1
    }

    assert(newData.length == i, s"new data length '$i' (expected: ${newData.length})")
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
    val builder = PKExistingEdgePartitionBuilder[V, E](this)

    var i = 0
    for (edge <- iterator) {
      // The user sees the EdgeTriplet, so we can't reuse it and must create one per edge.
      val triplet = new EdgeTriplet[V, E]
      triplet.srcId = edge.srcId
      triplet.dstId = edge.dstId
      triplet.srcAttr = vertexAttribute(triplet.srcId)
      triplet.dstAttr = vertexAttribute(triplet.dstId)
      triplet.attr = edge.attr

      if (vpred(triplet.srcId, triplet.srcAttr) && vpred(triplet.dstId, triplet.dstAttr) && epred(triplet)) {
        builder.addEdge(triplet.srcId, triplet.dstId, triplet.attr)
      }

      i += 1
    }

    builder.build()
  }

  /**
    * Apply the function f to all edges in this partition.
    *
    * @param f an external state mutating user defined function.
    */
  def foreach(f: Edge[E] => Unit): Unit = iterator.foreach(f)

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
    assert(other.tree.k == tree.k)
    val builder = PKExistingEdgePartitionBuilder[V, E3](this)

    // Optimization: check if the partitions have any intersection at all, in which case
    // the result of the inner join is always empty
    if (!partitionIntersects(other)) {
      return builder.build()
    }

    val comparator = new PKEdgeComparator[E, E2](this, other)
    val it1 = iterator
    val it2 = other.iterator

    var edge2: Edge[E2] = Edge(-1, -1, null.asInstanceOf[E2])
    while (it1.hasNext && it2.hasNext) {
      val edge1 = it1.next()

      while (it2.hasNext && comparator.compare(edge1, edge2)) {
        edge2 = it2.next()
      }

      if (edge1.srcId == edge2.srcId && edge1.dstId == edge2.dstId) {
        builder.addEdge(edge1.srcId, edge1.dstId, f(edge1.srcId, edge1.dstId, edge1.attr, edge2.attr))
      }
    }

    builder.build()
  }

  /**
    * The number of edges in this partition
    *
    * @return size of the partition
    */
  val size: Int = edgeAttrs.length

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

      override def hasNext: Boolean = pos < edgeAttrs.length && iterator.hasNext

      override def next(): Edge[E] = {
        val (line, col) = iterator.next()
        edge.srcId = line + srcOffset
        edge.dstId = col + dstOffset
        edge.attr = edgeAttrs(pos)
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
      private val iterator = PKEdgePartition.this.iterator

      override def hasNext: Boolean = iterator.hasNext

      override def next(): EdgeTriplet[V, E] = {
        val triplet = new EdgeTriplet[V, E]

        val edge = iterator.next()
        triplet.srcId = edge.srcId
        triplet.dstId = edge.dstId
        triplet.attr = edge.attr

        if (includeSrc) {
          triplet.srcAttr = vertexAttribute(edge.srcId)
        }

        if (includeDst) {
          triplet.dstAttr = vertexAttribute(edge.dstId)
        }

        triplet
      }
    }

  /**
    * Send messages along edges and aggregate them at the receiving vertices.
    *
    * @param sendMsg generates messages to neighboring vertices of an edge
    * @param mergeMsg the combiner applied to messages destined to the same vertex
    * @param tripletFields which triplet fields `sendMsg` uses
    *
    * @return iterator aggregated messages keyed by the receiving vertex id
    */
  def aggregateMessages[A: ClassTag](
      sendMsg: EdgeContext[V, E, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields,
      activeness: EdgeActiveness
  ): Iterator[(VertexId, A)] = {
    val ctx = new PKAggregatingEdgeContext[V, E, A](vertexAttrs.length, mergeMsg)
    for (edge <- iterator) {
      if (isEdgeActive(edge.srcId, edge.dstId, activeness)) {
        val srcAttr = if (tripletFields.useSrc) vertexAttribute(edge.srcId) else null.asInstanceOf[V]
        val dstAttr = if (tripletFields.useDst) vertexAttribute(edge.dstId) else null.asInstanceOf[V]
        ctx.set(edge.srcId, edge.dstId, global2local(edge.srcId), global2local(edge.dstId), srcAttr, dstAttr, edge.attr)
        sendMsg(ctx)
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
    new PKEdgePartition(vertexAttrs, global2local, attrs, tree, srcOffset, dstOffset, activeSet)
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
    * Checks if this partition as any "possible" intersecting edges with the 'other' partition.
    *
    * @param other   Other partition to test
    * @return true if there is an intersection, false otherwise
    */
  private def partitionIntersects(other: PKEdgePartition[_, _]): Boolean = {
    // Before or after in the vertical axis
    if (srcOffset > other.srcOffset + other.tree.size || srcOffset + tree.size < other.srcOffset) {
      return false
    }

    // Before or after in the horizontal axis
    if (dstOffset > other.dstOffset + other.tree.size || dstOffset + tree.size < other.dstOffset) {
      return false
    }

    true
  }
}

object PKEdgePartition {

  /**
    * Get an empty partition.
    *
    * @tparam V    Type of vertex attributes
    * @tparam E    Type of edge attributes
    * @return partition with no edges or cached vertices
    */
  def empty[V: ClassTag, E: ClassTag]: PKEdgePartition[V, E] =
    new PKEdgePartition[V, E](
      Array.empty,
      new GraphXPrimitiveKeyOpenHashMap[VertexId, Int],
      Array.empty,
      new K2Tree(0, 0, new Bitset(0), 0, Array.empty),
      0,
      0,
      None
    )
}
