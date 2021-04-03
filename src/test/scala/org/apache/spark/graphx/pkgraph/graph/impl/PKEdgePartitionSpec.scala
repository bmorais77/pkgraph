package org.apache.spark.graphx.pkgraph.graph.impl

import org.apache.spark.graphx.Edge
import org.scalatest.FlatSpec

class PKEdgePartitionSpec extends FlatSpec {
  "A PKEdgePartition" should "create a new partition without vertex attributes" in {
    val partition = buildPartition
    partition.updateVertices(Seq((0L, 10), (1L, 20)).iterator)
    assert(partition.vertexAttrs.size == 2)

    val partitionWithNoVerticesCached = partition.withoutVertexAttributes()
    assert(partitionWithNoVerticesCached.vertexAttrs.size == 0)
  }

  /**
   * Matrix 16x16:
   * +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
   * | 1   0   0   0 | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 |
   * | 0   1   0   0 | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 |
   * | 0   0   1   0 | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 |
   * | 0   0   0   1 | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 |
   * |---+---+---+---|---+---+---+---|---+---+---+---|---+---+---+---|
   * | 0   0   0   0 | 1   0   0   0 | 0   0   0   0 | 0   0   0   0 |
   * | 0   0   0   0 | 0   1   0   0 | 0   0   0   0 | 0   0   0   0 |
   * | 0   0   0   0 | 0   0   1   0 | 0   0   0   0 | 0   0   0   0 |
   * | 0   0   0   0 | 0   0   0   1 | 0   0   0   0 | 0   0   0   0 |
   * +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
   * | 0   0   0   0 | 0   0   0   0 | 1   0   0   0 | 0   0   0   0 |
   * | 0   0   0   0 | 0   0   0   0 | 0   1   0   0 | 0   0   0   0 |
   * | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 |
   * | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 |
   * |---+---+---+---|---+---+---+---|---+---+---+---|---+---+---+---|
   * | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 |
   * | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 |
   * | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 |
   * | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 | 0   0   0   0 |
   * +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
   *
   * T: 1001 1001 1000 1001 1001 1000
   * L: 1001 1001 1001 1001 1001
   */
  it should "add new edges" in {
    val partition = buildPartition
    assert(partition.size == 10)

    val newEdges = Seq(
      Edge[Int](10, 10, 10),
      Edge[Int](11, 11, 11),
      Edge[Int](12, 12, 12),
    )

    val newPartition = partition.addEdges(newEdges.iterator)
    assert(newPartition.size == partition.size + newEdges.length)

    var i = 0
    val it = newPartition.iterator
    while(it.hasNext) {
      val edge = it.next()
      assert(edge.srcId == i)
      assert(edge.dstId == i)
      assert(edge.attr == i)
      i += 1
    }
    assert(i == newPartition.size)
  }

  private def buildPartition: PKEdgePartition[Int, Int] = {
    val builder = PKEdgePartitionBuilder[Int, Int](2)
    for(i <- 0 until 10) {
      builder.add(i, i, i)
    }
    builder.build
  }
}
