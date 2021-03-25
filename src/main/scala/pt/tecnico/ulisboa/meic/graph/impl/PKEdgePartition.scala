package pt.tecnico.ulisboa.meic.graph.impl

import org.apache.spark.graphx.impl.EdgeActiveness
import org.apache.spark.graphx.{Edge, EdgeContext, EdgeTriplet, TripletFields, VertexId}
import pt.tecnico.ulisboa.meic.compression.K2Tree

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
    * Adapted from EdgePartition.scala:
    *
    * Return a new edge partition with the specified edge data.
    *
    * @param data Edge attributes
    * @tparam E2 new type of edges
    * @return edge partition with given edge data
    */
  def withData[E2: ClassTag](data: Array[E2]): PKEdgePartition[V, E2] = {
    new PKEdgePartition(vertexAttrs, data, tree, lineOffset, colOffset)
  }

  /**
    * Adapted from EdgePartition.scala:
    *
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

  /**
    * Adapted from EdgePartition.scala:
    *
    * Reverse all the edges in this partition.
    *
    * @return a new edge partition with all edges reversed.
    */
  def reverse: PKEdgePartition[V, E] = {
    new PKEdgePartition(vertexAttrs, edgeAttrs.reverse, tree.reverse, lineOffset, colOffset)
  }

  /**
    * Adapted from EdgePartition.scala:
    *
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

    this.withData(newData)
  }

  /**
    * Adapted from EdgePartition.scala:
    *
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
    * Adapted from EdgePartition.scala:
    *
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
    * Adapted from EdgePartition.scala:
    *
    * Apply the function f to all edges in this partition.
    *
    * @param f an external state mutating user defined function.
    */
  def foreach(f: Edge[E] => Unit): Unit = {
    iterator.foreach(f)
  }

  /**
    * Adapted from EdgePartition.scala:
    *
    * Merge all the edges with the same src and dest id into a single
    * edge using the `merge` function
    *
    * TODO: Because of our use of K²-Tree this function may be unnecessary? check if attributes are not repeated
    *
    * @param merge a commutative associative merge operation
    * @return a new edge partition without duplicate edges
    */
  def groupEdges(merge: (E, E) => E): PKEdgePartition[E, V] = ???

  /**
    * Adapted from EdgePartition.scala:
    *
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
    * Adapted from EdgePartition.scala:
    *
    * The number of edges in this partition
    *
    * TODO: Check if this function is actually needed, so we can avoid storing more metadata in the K²-Tree
    *
    * @return size of the partition
    */
  val size: Int = 0

  /**
    * Adapted from EdgePartition.scala:
    *
    * TODO: Implement
    *
    * @return The number of unique source vertices in the partition.
    */
  def indexSize: Int = 0

  /**
    * Adapted from EdgePartition.scala:
    *
    * Get an iterator over the edges in this partition.
    *
    * Be careful not to keep references to the objects from this iterator.
    * To improve GC performance the same object is re-used in `next()`.
    *
    * TODO: Use an K²-Tree iterator instead of collecting all edges
    *
    * @return an iterator over edges in the partition
    */
  def iterator: Iterator[Edge[E]] =
    new Iterator[Edge[E]] {
      private val edges = tree.edges
      private val edge = new Edge[E]
      private var pos = 0

      override def hasNext: Boolean = pos < edges.length

      override def next(): Edge[E] = {
        edge.srcId = edges(pos)._1
        edge.dstId = edges(pos)._2
        edge.attr = edgeAttrs(pos)
        pos += 1
        edge
      }
    }

  /**
    * Adapted from EdgePartition.scala:
    *
    * Get an iterator over the edge triplets in this partition.
    *
    * It is safe to keep references to the objects from this iterator.
    *
    * TODO: Use an K²-Tree iterator instead of collecting all edges
    *
    * @param includeSrc Include source vertex attributes
    * @param includeDst Include destination vertex
    * @return edge triplet iterator
    */
  def tripletIterator(includeSrc: Boolean = true, includeDst: Boolean = true): Iterator[EdgeTriplet[V, E]] =
    new Iterator[EdgeTriplet[V, E]] {
      private val edges = tree.edges
      private var pos = 0

      override def hasNext: Boolean = pos < edges.length

      override def next(): EdgeTriplet[V, E] = {
        val triplet = new EdgeTriplet[V, E]

        val (line, col) = edges(pos)
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

  // TODO: Not sure if aggregate message functions are necessary

  /**
    * TODO: Implement
    *
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
}
