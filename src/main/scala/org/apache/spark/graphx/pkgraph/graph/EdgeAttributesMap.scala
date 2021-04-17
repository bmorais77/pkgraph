package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.util.collection.BitSet

import scala.reflect.ClassTag

class EdgeAttributesMap[@specialized(Long, Int, Double) E: ClassTag](
    val indices: BitSet,
    val values: Array[E]
) extends Iterable[(Int, E)] {
  def iterator: Iterator[(Int, E)] =
    new Iterator[(Int, E)] {
      private var pos = -1
      private var index = 0

      override def hasNext: Boolean = pos < values.length

      override def next(): (Int, E) = {
        index = indices.nextSetBit(index)
        pos += 1
        (index, values(pos))
      }
    }

  def iterable: EdgeAttributesIterable[E] = new EdgeAttributesIterable[E](this)
}
