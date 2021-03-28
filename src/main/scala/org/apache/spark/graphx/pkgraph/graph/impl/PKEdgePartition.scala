package org.apache.spark.graphx.pkgraph.graph.impl

import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.EdgeActiveness
import org.apache.spark.graphx.pkgraph.compression.K2Tree

import scala.collection.mutable
import scala.reflect.ClassTag

class PKEdgePartition[V: ClassTag, E: ClassTag](
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

  // TODO: Maybe implement an existing partition builder

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

    tree.forEachEdge((line, col) => {
      edge.srcId = line + lineOffset
      edge.dstId = col + colOffset
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
    // TODO: Optimize using an ExistingPKEdgePartitionBuilder to reuse already computed data
    val builder = new PKEdgePartitionBuilder[V, E](tree.k)
    var i = 0

    tree.forEachEdge((line, col) => {
      // The user sees the EdgeTriplet, so we can't reuse it and must create one per edge.
      val triplet = new EdgeTriplet[V, E]
      triplet.srcId = line + lineOffset
      triplet.dstId = col + colOffset
      triplet.srcAttr = vertexAttrs(line + lineOffset)
      triplet.dstAttr = vertexAttrs(col + colOffset)
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

  // TODO: When adding an edge, check if its repeated, if true then merge attributes
  // TODO: May need to order edges before adding

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
  def innerJoin[E2, E3](other: PKEdgePartition[_, E2])(f: (VertexId, VertexId, E, E2) => E3): PKEdgePartition[V, E3] = {
    // TODO: Implement a inner join on the K²-Tree
    ???
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
  def iterator: Iterator[Edge[E]] = new Iterator[Edge[E]] {
      private val iterator = tree.iterator
      private val edge = new Edge[E]
      private var pos = 0

      override def hasNext: Boolean = iterator.hasNext

      override def next(): Edge[E] = {
        val (line, col) = iterator.next()
        edge.srcId = line + lineOffset
        edge.dstId = col + colOffset
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

        val (line, col) = iterator.next()
        triplet.srcId = line + lineOffset
        triplet.dstId = col + colOffset
        triplet.attr = edgeAttrs(pos)

        if (includeSrc) {
          triplet.srcAttr = vertexAttrs(line + lineOffset)
        }

        if (includeDst) {
          triplet.dstAttr = vertexAttrs(col + colOffset)
        }

        pos += 1
        triplet
      }
    }

  // TODO: Implement

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
  ): Iterator[(VertexId, A)] = ???

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
