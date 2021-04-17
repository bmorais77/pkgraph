package org.apache.spark.graphx.pkgraph.graph

import scala.reflect.ClassTag

class EdgeAttributesIterable[E: ClassTag](attrs: EdgeAttributesMap[E]) {
  private var pos = 0
  private var currIndex = 0

  def nextAttribute(index: Int): E = {
    while (currIndex != index) {
      currIndex = attrs.indices.nextSetBit(currIndex)
      pos += 1
    }

    if (index != currIndex) {
      throw new Exception(s"key $index does not exist in EdgeAttributesMap")
    }

    attrs.values(pos)
  }
}
