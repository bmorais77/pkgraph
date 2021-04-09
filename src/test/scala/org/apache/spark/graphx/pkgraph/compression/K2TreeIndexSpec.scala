package org.apache.spark.graphx.pkgraph.compression

import org.scalatest.FlatSpec

class K2TreeIndexSpec extends FlatSpec {
  "A K2TreeIndex" should "build from edge" in {
    val index = K2TreeIndex.fromEdge(2, 3, 3, 3)
    assert(index == 15)
  }

  it should "compare with other equal indices with same size" in {
    val i1 = K2TreeIndex.fromEdge(2, 3, 4, 4)
    val i2 = K2TreeIndex.fromEdge(2, 3, 4, 4)
    assert(i1 == i2)
  }

  it should "compare with other equal indices with different size" in {
    val i1 = K2TreeIndex.fromEdge(2, 3, 4, 4)
    val i2 = K2TreeIndex.fromEdge(2, 4, 4, 4)
    assert(i1 == i2)
  }

  it should "compare with other indices of same size" in {
    val i1 = K2TreeIndex.fromEdge(2, 3, 3, 3)
    val i2 = K2TreeIndex.fromEdge(2, 3, 4, 4)
    assert(i1 < i2)
  }

  it should "compare with other indices of different size" in {
    val i1 = K2TreeIndex.fromEdge(2, 3, 4, 3)
    val i2 = K2TreeIndex.fromEdge(2, 4, 4, 4)
    assert(i1 < i2)
  }
}
