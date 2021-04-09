package org.apache.spark.graphx.pkgraph.graph.impl

import org.apache.spark.graphx.{PartitionID, VertexId}
import org.apache.spark.graphx.impl.VertexAttributeBlock
import org.apache.spark.graphx.pkgraph.util.collection.PrimitiveHashMap
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.util.collection.{BitSet, OpenHashSet, PrimitiveVector}

import scala.reflect.ClassTag

/**
  * A map from vertex id to vertex attribute that additionally stores edge partition join sites for
  * each vertex attribute, enabling joining with an [[org.apache.spark.graphx.EdgeRDD]].
  */
private[impl] class PKVertexPartition[@specialized(Long, Int, Double) V: ClassTag](
    val index: OpenHashSet[VertexId],
    val values: Array[V],
    val mask: BitSet,
    val routingTable: PKRoutingTablePartition
) extends Serializable {

  val capacity: Int = index.capacity

  def size: Int = mask.cardinality()

  /** Return the vertex attribute for the given vertex ID. */
  def apply(vid: VertexId): V = values(index.getPos(vid))

  def isDefined(vid: VertexId): Boolean = {
    val pos = index.getPos(vid)
    pos >= 0 && mask.get(pos)
  }

  def iterator: Iterator[(VertexId, V)] = mask.iterator.map(ind => (index.getValue(ind), values(ind)))

  /**
    * Pass each vertex attribute along with the vertex id through a map
    * function and retain the original RDD's partitioning and index.
    *
    * @tparam V2 the type returned by the map function
    *
    * @param f the function applied to each vertex id and vertex
    * attribute in the RDD
    *
    * @return a new VertexPartition with values obtained by applying `f` to
    * each of the entries in the original VertexRDD.  The resulting
    * VertexPartition retains the same index.
    */
  def map[V2: ClassTag](f: (VertexId, V) => V2): PKVertexPartition[V2] = {
    // Construct a view of the map transformation
    val newValues = new Array[V2](capacity)
    var i = mask.nextSetBit(0)
    while (i >= 0) {
      newValues(i) = f(index.getValue(i), values(i))
      i = mask.nextSetBit(i + 1)
    }
    withValues(newValues)
  }

  /**
    * Restrict the vertex set to the set of vertices satisfying the given predicate.
    *
    * @param pred the user defined predicate
    *
    * @note The vertex set preserves the original index structure which means that the returned
    *       RDD can be easily joined with the original vertex-set. Furthermore, the filter only
    *       modifies the bitmap index and so no new values are allocated.
    */
  def filter(pred: (VertexId, V) => Boolean): PKVertexPartition[V] = {
    // Allocate the array to store the results into
    val newMask = new BitSet(capacity)
    // Iterate over the active bits in the old mask and evaluate the predicate
    var i = mask.nextSetBit(0)
    while (i >= 0) {
      if (pred(index.getValue(i), values(i))) {
        newMask.set(i)
      }
      i = mask.nextSetBit(i + 1)
    }
    withMask(newMask)
  }

  /** Hides the VertexId's that are the same between `this` and `other`. */
  def minus(other: PKVertexPartition[V]): PKVertexPartition[V] = {
    if (index != other.index) {
      minus(createUsingIndex(other.iterator))
    } else {
      withMask(mask.andNot(other.mask))
    }
  }

  /** Hides the VertexId's that are the same between `this` and `other`. */
  def minus(other: Iterator[(VertexId, V)]): PKVertexPartition[V] = {
    minus(createUsingIndex(other))
  }

  /**
    * Hides vertices that are the same between this and other. For vertices that are different, keeps
    * the values from `other`. The indices of `this` and `other` must be the same.
    */
  def diff(other: PKVertexPartition[V]): PKVertexPartition[V] = {
    if (index != other.index) {
      diff(createUsingIndex(other.iterator))
    } else {
      val newMask = mask & other.mask
      var i = newMask.nextSetBit(0)
      while (i >= 0) {
        if (values(i) == other.values(i)) {
          newMask.unset(i)
        }
        i = newMask.nextSetBit(i + 1)
      }
      withValues(other.values).withMask(newMask)
    }
  }

  /** Left outer join another VertexPartition. */
  def leftJoin[V2: ClassTag, V3: ClassTag](
      other: PKVertexPartition[V2]
  )(f: (VertexId, V, Option[V2]) => V3): PKVertexPartition[V3] = {
    if (index != other.index) {
      leftJoin(createUsingIndex(other.iterator))(f)
    } else {
      val newValues = new Array[V3](capacity)

      var i = mask.nextSetBit(0)
      while (i >= 0) {
        val otherV: Option[V2] = if (other.mask.get(i)) Some(other.values(i)) else None
        newValues(i) = f(index.getValue(i), values(i), otherV)
        i = mask.nextSetBit(i + 1)
      }
      this.withValues(newValues)
    }
  }

  /** Left outer join another iterator of messages. */
  def leftJoin[V2: ClassTag, V3: ClassTag](
      other: Iterator[(VertexId, V2)]
  )(f: (VertexId, V, Option[V2]) => V3): PKVertexPartition[V3] = {
    leftJoin(createUsingIndex(other))(f)
  }

  /** Inner join another VertexPartition. */
  def innerJoin[U: ClassTag, V2: ClassTag](
      other: PKVertexPartition[U]
  )(f: (VertexId, V, U) => V2): PKVertexPartition[V2] = {
    if (index != other.index) {
      innerJoin(createUsingIndex(other.iterator))(f)
    } else {
      val newMask = mask & other.mask
      val newValues = new Array[V2](capacity)
      var i = newMask.nextSetBit(0)
      while (i >= 0) {
        newValues(i) = f(index.getValue(i), values(i), other.values(i))
        i = newMask.nextSetBit(i + 1)
      }
      withValues(newValues).withMask(newMask)
    }
  }

  /**
    * Inner join an iterator of messages.
    */
  def innerJoin[U: ClassTag, V2: ClassTag](
      iter: Iterator[Product2[VertexId, U]]
  )(f: (VertexId, V, U) => V2): PKVertexPartition[V2] = {
    innerJoin(createUsingIndex(iter))(f)
  }

  /**
    * Similar effect as aggregateUsingIndex((a, b) => a)
    */
  def createUsingIndex[V2: ClassTag](iter: Iterator[Product2[VertexId, V2]]): PKVertexPartition[V2] = {
    val newMask = new BitSet(capacity)
    val newValues = new Array[V2](capacity)
    iter.foreach { pair =>
      val pos = index.getPos(pair._1)
      if (pos >= 0) {
        newMask.set(pos)
        newValues(pos) = pair._2
      }
    }
    withValues(newValues).withMask(newMask)
  }

  /**
    * Similar to innerJoin, but vertices from the left side that don't appear in iter will remain in
    * the partition, hidden by the bitmask.
    */
  def innerJoinKeepLeft(iter: Iterator[Product2[VertexId, V]]): PKVertexPartition[V] = {
    val newMask = new BitSet(capacity)
    val newValues = new Array[V](capacity)
    System.arraycopy(values, 0, newValues, 0, newValues.length)
    iter.foreach { pair =>
      val pos = index.getPos(pair._1)
      if (pos >= 0) {
        newMask.set(pos)
        newValues(pos) = pair._2
      }
    }
    this.withValues(newValues).withMask(newMask)
  }

  def aggregateUsingIndex[V2: ClassTag](
      iter: Iterator[Product2[VertexId, V2]],
      reduceFunc: (V2, V2) => V2
  ): PKVertexPartition[V2] = {
    val newMask = new BitSet(capacity)
    val newValues = new Array[V2](capacity)
    iter.foreach { product =>
      val vid = product._1
      val vdata = product._2
      val pos = index.getPos(vid)
      if (pos >= 0) {
        if (newMask.get(pos)) {
          newValues(pos) = reduceFunc(newValues(pos), vdata)
        } else { // otherwise just store the new value
          newMask.set(pos)
          newValues(pos) = vdata
        }
      }
    }
    withValues(newValues).withMask(newMask)
  }

  /**
    * Construct a new VertexPartition whose index contains only the vertices in the mask.
    */
  def reindex(): PKVertexPartition[V] = {
    val hashMap = new GraphXPrimitiveKeyOpenHashMap[VertexId, V]
    val arbitraryMerge = (a: V, _: V) => a
    for ((k, v) <- iterator) {
      hashMap.setMerge(k, v, arbitraryMerge)
    }
    withIndex(hashMap.keySet).withValues(hashMap._values).withMask(hashMap.keySet.getBitSet)
  }

  /**
    * Generate a `VertexAttributeBlock` for each edge partition keyed on the edge partition ID. The
    * `VertexAttributeBlock` contains the vertex attributes from the current partition that are
    * referenced in the specified positions in the edge partition.
    */
  def shipVertexAttributes(shipSrc: Boolean, shipDst: Boolean): Iterator[(PartitionID, PKVertexAttributeBlock[V])] = {
    Iterator.tabulate(routingTable.numEdgePartitions) { pid =>
      val initialSize = if (shipSrc && shipDst) routingTable.partitionSize(pid) else 64
      val vids = new PrimitiveVector[VertexId](initialSize)
      val attrs = new PrimitiveVector[V](initialSize)
      routingTable.foreachWithinEdgePartition(pid, shipSrc, shipDst) { vid =>
        if (isDefined(vid)) {
          vids += vid
          attrs += this(vid)
        }
      }
      (pid, new PKVertexAttributeBlock(vids.trim().array, attrs.trim().array))
    }
  }

  /**
    * Generate a `VertexId` array for each edge partition keyed on the edge partition ID. The array
    * contains the visible vertex ids from the current partition that are referenced in the edge
    * partition.
    */
  def shipVertexIds(): Iterator[(PartitionID, Array[VertexId])] = {
    Iterator.tabulate(routingTable.numEdgePartitions) { pid =>
      val vids = new PrimitiveVector[VertexId](routingTable.partitionSize(pid))
      routingTable.foreachWithinEdgePartition(pid, includeSrc = true, includeDst = true) { vid =>
        if (isDefined(vid)) {
          vids += vid
        }
      }
      (pid, vids.trim().array)
    }
  }

  def withIndex(index: OpenHashSet[VertexId]): PKVertexPartition[V] = {
    new PKVertexPartition(index, values, mask, routingTable)
  }

  def withValues[V2: ClassTag](values: Array[V2]): PKVertexPartition[V2] = {
    new PKVertexPartition[V2](index, values, mask, routingTable)
  }

  def withMask(mask: BitSet): PKVertexPartition[V] = {
    new PKVertexPartition(index, values, mask, routingTable)
  }

  def withRoutingTable(partition: PKRoutingTablePartition): PKVertexPartition[V] = {
    new PKVertexPartition(index, values, mask, partition)
  }
}

private[impl] object PKVertexPartition {

  /** Construct a `ShippableVertexPartition` from the given vertices without any routing table. */
  def apply[V: ClassTag](iter: Iterator[(VertexId, V)]): PKVertexPartition[V] =
    apply(iter, PKRoutingTablePartition.empty, null.asInstanceOf[V], (a, _) => a)

  /**
    * Construct a `ShippableVertexPartition` from the given vertices with the specified routing
    * table, filling in missing vertices mentioned in the routing table using `defaultVal`.
    */
  def apply[V: ClassTag](
      iter: Iterator[(VertexId, V)],
      routingTable: PKRoutingTablePartition,
      defaultVal: V
  ): PKVertexPartition[V] = apply(iter, routingTable, defaultVal, (a, _) => a)

  /**
    * Construct a `ShippableVertexPartition` from the given vertices with the specified routing
    * table, filling in missing vertices mentioned in the routing table using `defaultVal`,
    * and merging duplicate vertex attribute with mergeFunc.
    */
  def apply[V: ClassTag](
      iter: Iterator[(VertexId, V)],
      routingTable: PKRoutingTablePartition,
      defaultVal: V,
      mergeFunc: (V, V) => V
  ): PKVertexPartition[V] = {
    val map = new PrimitiveHashMap[VertexId, V]

    // Merge the given vertices using mergeFunc
    iter.foreach { pair =>
      map.setMerge(pair._1, pair._2, mergeFunc)
    }
    // Fill in missing vertices mentioned in the routing table
    routingTable.iterator.foreach { vid =>
      map.changeValue(vid, defaultVal, identity)
    }

    new PKVertexPartition(map.keySet, map._values, map.keySet.getBitSet, routingTable)
  }
}
