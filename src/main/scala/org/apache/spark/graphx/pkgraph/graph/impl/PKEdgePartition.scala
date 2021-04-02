package org.apache.spark.graphx.pkgraph.graph.impl

import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.EdgeActiveness
import org.apache.spark.graphx.pkgraph.compression.{K2Tree, K2TreeIndex}
import org.apache.spark.graphx.pkgraph.util.mathx

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

private[graph] class PKEdgePartition[V: ClassTag, E: ClassTag](
    vertexAttrs: Map[VertexId, V],
    edgeAttrs: Array[E],
    tree: K2Tree,
    lineOffset: Long,
    colOffset: Long
) {

  /**
    * Return a new edge partition without any locally cached vertex attributes.
    *
    * @tparam V2 new type of vertices
    * @return edge partition without cached vertices
    */
  def withoutVertexAttributes[V2: ClassTag](): PKEdgePartition[V2, E] = {
    new PKEdgePartition(Map.empty[VertexId, V2], edgeAttrs, tree, lineOffset, colOffset)
  }

  /**
    * Returns a new [[PKEdgePartition]] with the given edges added.
    *
    * @param edges Edges to add
    * @return new partition with edges added
    */
  def addEdges(edges: Iterator[Edge[E]]): PKEdgePartition[V, E] = {
    var newLineOffset = lineOffset
    var newColOffset = colOffset
    var newLineEnd = lineOffset + tree.size
    var newColEnd = colOffset + tree.size

    // Traverse edges to check if there are any behind the virtual origin (lineOffset, colOffset) or
    // after the size of the current matrix
    val newEdges = new ArrayBuffer[Edge[E]]
    for(edge <- edges) {
      newLineOffset = math.min(edge.srcId, lineOffset)
      newColOffset = math.min(edge.dstId, colOffset)
      newLineEnd = math.max(edge.srcId, newLineEnd)
      newColEnd = math.max(edge.dstId, newColEnd)
      newEdges += edge
    }

    var newTree = tree

    // Check if new edges are behind virtual origin
    if(newLineOffset < lineOffset || newColOffset < colOffset) {
      // TODO: Grow K²-Tree to the left and up
      newTree = tree
    }

    // Check if new edges are after the end of the adjacency matrix
    if(newLineEnd > lineOffset + tree.size || newColEnd > colOffset + tree.size) {
      // We need to first grow the tree
      var newSize = math.max(newLineEnd - newLineOffset, newColEnd - newColOffset).toInt

      // Size is not a power of K, so we need to find the nearest power
      if (newSize % tree.k != 0) {
        val exp = math.ceil(mathx.log(tree.k, newSize))
        newSize = math.pow(tree.k, exp).toInt
      }

      newTree = tree.grow(newSize)
    }

    // Add existing edges
    var i = 0
    val newEdgeAttrs = new mutable.TreeSet[(K2TreeIndex, E)]()((a, b) => a._1.compare(b._1))
    for(edge <- newTree.iterator) {
      newEdgeAttrs.add((edge.index, edgeAttrs(i)))
      i += 1
    }

    val builder = newTree.toBuilder

    // Traverse edges again and add them to the builder
    for(edge <- newEdges) {
      val line = (edge.srcId - newLineOffset).toInt
      val col = (edge.dstId - newColOffset).toInt
      builder.addEdge(line, col)
      newEdgeAttrs.add((K2TreeIndex.fromEdge(builder.k, builder.height, line, col), edge.attr))
    }

    val attrs = newEdgeAttrs.map(_._2).toArray
    new PKEdgePartition[V, E](vertexAttrs, attrs, builder.build, newLineOffset, newColOffset)
  }

  /**
    * Returns new [[PKEdgePartition]] with the given edges removed.
    *
    * @param edges Edges to remove
    * @return new partition with edges removed
    */
  def removeEdges(edges: Iterator[Edge[E]]): PKEdgePartition[V, E] = {
    // TODO: Implement
    this
  }

  /**
    * Adapted from EdgePartition.scala:
    *
    * Updates the vertex attributes in this partition with the ones in the given iterator.
    *
    * @param iter Iterator with vertex attributes
    * @return
    */
  def updateVertices(iter: Iterator[(VertexId, V)]): PKEdgePartition[V, E] = {
    val newVertexAttrs = new mutable.OpenHashMap[VertexId, V]() ++= vertexAttrs
    while (iter.hasNext) {
      val (id, attr) = iter.next()
      newVertexAttrs(id) = attr
    }
    new PKEdgePartition(newVertexAttrs.toMap, edgeAttrs, tree, lineOffset, colOffset)
  }

  /**
    * Reverse all the edges in this partition.
    *
    * @return a new edge partition with all edges reversed.
    */
  def reverse: PKEdgePartition[V, E] = {
    new PKEdgePartition(vertexAttrs, edgeAttrs.reverse, tree.reverse, lineOffset, colOffset)
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
    val edge = new Edge[E]()
    var i = 0

    tree.forEachEdge(e => {
      edge.srcId = e.line + lineOffset
      edge.dstId = e.col + colOffset
      edge.attr = edgeAttrs(i)
      newData(i) = f(edge)
      i += 1
    })

    withData(newData)
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
    assert(newData.length == i)
    this.withData(newData)
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
    val builder = new PKEdgePartitionBuilder[V, E](tree.k)
    var i = 0

    tree.forEachEdge(e => {
      // The user sees the EdgeTriplet, so we can't reuse it and must create one per edge.
      val triplet = new EdgeTriplet[V, E]
      triplet.srcId = e.line + lineOffset
      triplet.dstId = e.col + colOffset
      triplet.srcAttr = vertexAttrs(e.line + lineOffset)
      triplet.dstAttr = vertexAttrs(e.col + colOffset)
      triplet.attr = edgeAttrs(i)

      if (vpred(triplet.srcId, triplet.srcAttr) && vpred(triplet.dstId, triplet.dstAttr) && epred(triplet)) {
        builder.add(triplet.srcId, triplet.dstId, triplet.attr)
      }

      i += 1
    })

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
    val builder = new PKEdgePartitionBuilder[V, E3](tree.k)
    val it1 = iteratorWithIndex
    val it2 = other.iteratorWithIndex

    while (it1.hasNext && it2.hasNext) {
      val edge1 = it1.next()
      var edge2 = it2.next()

      while (it2.hasNext && edge2.index < edge1.index) {
        edge2 = it2.next()
      }

      if (edge1.line == edge2.line && edge1.col == edge2.col) {
        builder.add(edge1.line, edge1.col, f(edge1.line, edge1.col, edge1.attr, edge2.attr))
      }
    }

    builder.build
  }

  /**
    * The number of edges in this partition
    *
    * @return size of the partition
    */
  val size: Int = tree.edgeCount

  /**
    * Get an iterator over the edges in this partition.
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

      override def hasNext: Boolean = iterator.hasNext

      override def next(): Edge[E] = {
        val nextEdge = iterator.next()
        edge.srcId = nextEdge.line + lineOffset
        edge.dstId = nextEdge.col + colOffset
        edge.attr = edgeAttrs(pos)
        pos += 1
        edge
      }
    }

  /**
    * Get an iterator over the edges in this partition with the edge index included.
    *
    * Be careful not to keep references to the objects from this iterator.
    * To improve GC performance the same object is re-used in `next()`.
    *
    * @return an iterator over edges in the partition
    */
  def iteratorWithIndex: Iterator[PKEdge[E]] =
    new Iterator[PKEdge[E]] {
      private val iterator = tree.iterator
      private val edge = new PKEdge[E]
      private var pos = 0

      override def hasNext: Boolean = iterator.hasNext

      override def next(): PKEdge[E] = {
        val nextEdge = iterator.next()
        edge.index = nextEdge.index
        edge.line = nextEdge.line + lineOffset
        edge.col = nextEdge.col + colOffset
        edge.attr = edgeAttrs(pos)
        pos += 1
        edge
      }
    }

  /**
    * Get an iterator over the edge triplets in this partition.
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

      override def hasNext: Boolean = iterator.hasNext

      override def next(): EdgeTriplet[V, E] = {
        val triplet = new EdgeTriplet[V, E]

        val edge = iterator.next()
        triplet.srcId = edge.line + lineOffset
        triplet.dstId = edge.col + colOffset
        triplet.attr = edgeAttrs(pos)

        if (includeSrc) {
          triplet.srcAttr = vertexAttrs(edge.line + lineOffset)
        }

        if (includeDst) {
          triplet.dstAttr = vertexAttrs(edge.col + colOffset)
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
    * @param activeness criteria for filtering edges based on activeness
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
    for(edge <- iterator) {
      // TODO: Maybe support EdgeActiveness?
      val srcAttr = if (tripletFields.useSrc) vertexAttrs(edge.srcId) else null.asInstanceOf[V]
      val dstAttr = if (tripletFields.useDst) vertexAttrs(edge.dstId) else null.asInstanceOf[V]
      ctx.set(edge.srcId, edge.dstId, srcAttr, dstAttr, edge.attr)
      sendMsg(ctx)
    }
    ctx.iterator
  }

  /**
    * Return a new edge partition with the specified edge data.
    *
    * @param data Edge attributes
    * @tparam E2 new type of edges
    * @return edge partition with given edge data
    */
  private def withData[E2: ClassTag](data: Array[E2]): PKEdgePartition[V, E2] = {
    new PKEdgePartition(vertexAttrs, data, tree, lineOffset, colOffset)
  }
}
