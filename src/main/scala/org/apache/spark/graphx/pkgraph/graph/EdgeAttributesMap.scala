package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap

import scala.reflect.ClassTag

class EdgeAttributesMap[@specialized(Long, Int, Double) E: ClassTag](
    val indices: GraphXPrimitiveKeyOpenHashMap[Int, Int],
    val values: Array[E]
) extends Iterable[(Int, E)] {
  override def size: Int = values.length

  def getAttributeWithIndex(index: Int): E = values(indices(index))

  def iterator: Iterator[(Int, E)] = indices.iterator.map{ case (k, v) => (k, values(v)) }
}

object EdgeAttributesMap {
  def empty[E: ClassTag]: EdgeAttributesMap[E] =
    new EdgeAttributesMap(new GraphXPrimitiveKeyOpenHashMap[Int, Int](), Array.empty)
}
