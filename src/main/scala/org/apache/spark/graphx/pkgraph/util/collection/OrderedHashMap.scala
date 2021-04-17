package org.apache.spark.graphx.pkgraph.util.collection

import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap

import scala.reflect.ClassTag

class OrderedHashMap[@specialized(Long, Int) K: ClassTag, @specialized(Long, Int, Double) V: ClassTag](
    val indices: GraphXPrimitiveKeyOpenHashMap[K, Int],
    val values: Array[V]
)(implicit val ord: Ordering[K]) extends Iterable[(K, V)] {
  def apply(key: K): V = {
    val pos = indices(key)
    values(pos)
  }

  def iterator: Iterator[(K, V)] = {
    val sortedIndices = indices.toArray.sorted
    new Iterator[(K, V)] {
      private var i = 0

      override def hasNext: Boolean = i < sortedIndices.length

      override def next(): (K, V) = {
        val (index, nextPos) = sortedIndices(i)
        i += 1
        (index, values(nextPos))
      }
    }
  }
}
