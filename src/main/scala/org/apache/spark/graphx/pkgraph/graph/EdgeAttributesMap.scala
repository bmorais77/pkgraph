package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.util.collection.BitSet

import scala.reflect.ClassTag

class EdgeAttributesMap[@specialized(Long, Int, Double) E: ClassTag](
    val indices: BitSet,
    val values: Array[E]
) extends Iterable[(Int, E)] {
  override def size: Int = values.length

  def iterator: Iterator[(Int, E)] =
    new Iterator[(Int, E)] {
      private var pos = 0
      private var index = -1

      override def hasNext: Boolean = pos < values.length

      override def next(): (Int, E) = {
        index = indices.nextSetBit(index + 1)
        val next = (index, values(pos))
        pos += 1
        next
      }
    }

  def iterable: EdgeAttributesIterable[E] = new EdgeAttributesIterable[E](this)
}

object EdgeAttributesMap {
  def empty[E: ClassTag]: EdgeAttributesMap[E] = new EdgeAttributesMap(new BitSet(0), Array.empty)
}
