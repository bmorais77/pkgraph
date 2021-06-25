package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.graphx.impl.EdgeActiveness
import org.apache.spark.graphx.{Edge, TripletFields}
import org.scalatest.FlatSpec

class PKEdgePartitionSpec extends FlatSpec {
  "A PKEdgePartition" should "create a new partition without vertex attributes" in {
    var partition = buildTestPartition
    partition = partition.updateVertices(Seq((0L, 10), (1L, 20)).iterator)
    assert(partition.vertexAttrs.count(i => i != 0) == 2)

    val partitionWithNoVerticesCached = partition.withoutVertexAttributes[Int]()
    assert(partitionWithNoVerticesCached.vertexAttrs.count(i => i != 0) == 0)
  }

  it should "create a new partition with large size" in {
    val size = 50000
    val partition = buildEdgePartition(2, size)
    assert(partition.size == size)
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
    val partition = buildTestPartition
    val newEdges = Seq(
      Edge[Int](10, 10, 10),
      Edge[Int](11, 11, 11),
      Edge[Int](12, 12, 12)
    )

    val newPartition = partition.addEdges(newEdges.iterator)
    assert(newPartition.size == partition.size + newEdges.length)

    var i = 0
    val it = newPartition.iterator
    while (it.hasNext) {
      val edge = it.next()
      assert(edge.srcId == i)
      assert(edge.dstId == i)
      assert(edge.attr == i)
      i += 1
    }
    assert(i == newPartition.size)
  }

  /**
   * Before:
   * Matrix 4x4:
   * +---+---+---+---+
   * | 1   1   0   0 |
   * | 1   0   0   0 |
   * | 0   0   1   0 |
   * | 0   0   0   0 |
   * +---+---+---+---+
   *
   * T: 1001
   * L: 1110 1000
   *
   * After:
   * Matrix 8x8:
   * +---+---+---+---+---+---+---+---+
   * | 0   0   0   0   0   0   0   0 |
   * | 0   1   1   0   0   0   0   0 |
   * | 0   1   0   0   0   0   0   0 |
   * | 0   0   0   0   0   0   0   0 |
   * | 0   0   0   0   1   1   0   0 |
   * | 0   0   0   0   1   0   0   0 |
   * | 0   0   0   0   0   0   1   0 |
   * | 0   0   0   0   0   0   0   0 |
   * +---+---+---+---+---+---+---+---+
   *
   * T: 1001 1110 1001
   * L: 0001 0010 0100 1110 1000
   */
  it should "add new edges behind origin" in {
    val builder = PKEdgePartitionBuilder[Int, Int](2)
    val existingEdges = Array(
      Edge[Int](4, 4, 4 * 4),
      Edge[Int](6, 6, 6 * 6),
      Edge[Int](4, 5, 4 * 5),
      Edge[Int](5, 4, 5 * 4),
    )

    for(edge <- existingEdges) {
      builder.add(edge.srcId, edge.dstId, edge.attr)
    }

    val partition = builder.build
    val newEdges = Array(
      Edge[Int](1, 1, 1),
      Edge[Int](1, 2, 2),
      Edge[Int](2, 1, 2),
      Edge[Int](5, 5, 25),
    )

    val newPartition = partition.addEdges(newEdges.iterator)
    assert(newPartition.size == partition.size + newEdges.length)

    var i = 0
    val it = newPartition.iterator
    while (it.hasNext) {
      val edge = it.next()
      assert(existingEdges.contains(edge) || newEdges.contains(edge))
      assert(edge.attr == edge.srcId * edge.dstId)
      i += 1
    }
    assert(i == newPartition.size)
  }

  it should "remove edges" in {
    val partition = buildTestPartition
    val edgesToRemove = Seq((0L, 0L), (1L, 1L), (2L, 2L))
    val newPartition = partition.removeEdges(edgesToRemove.toIterator)
    assert(newPartition.size == partition.size - edgesToRemove.length)

    var i = 3L
    val it = newPartition.iterator
    while (it.hasNext) {
      val edge = it.next()
      assert(edge.srcId == i)
      assert(edge.dstId == i)
      assert(edge.attr == i)
      i += 1
    }
    assert(i - edgesToRemove.length == newPartition.size)
  }

  it should "update vertices" in {
    val partition = buildTestPartition
    val vertices = (0 until 10).map(i => (i.toLong, i * 10))
    val newPartition = partition.updateVertices(vertices.iterator)
    assert(newPartition.size == partition.size)
    assert(newPartition.vertexAttrs.length == vertices.length)

    var i = 0
    val it = newPartition.tripletIterator()
    while (it.hasNext && i < vertices.length) {
      val edge = it.next()
      assert(edge.srcId == i)
      assert(edge.dstId == i)
      assert(edge.attr == i)
      assert(edge.srcAttr == i * 10)
      assert(edge.dstAttr == i * 10)
      i += 1
    }

    assert(i == vertices.length)
  }

  it should "reverse partition" in {
    val builder = PKEdgePartitionBuilder[Int, Int](2)
    for (i <- 0 until 10) {
      builder.add(i, i + 1, i)
    }

    val partition = builder.build
    var i = 0
    val it = partition.iterator
    while (it.hasNext) {
      val edge = it.next()
      assert(edge.srcId == i)
      assert(edge.dstId == i + 1)
      assert(edge.attr == i)
      i += 1
    }
    assert(i == partition.size)

    val reversedPartition = partition.reverse
    i = 0

    val reversedIt = reversedPartition.iterator
    while (reversedIt.hasNext) {
      val edge = reversedIt.next()
      assert(edge.srcId == i + 1)
      assert(edge.dstId == i)
      assert(edge.attr == i)
      i += 1
    }

    assert(i == partition.size)
  }

  it should "map edge attributes" in {
    val partition = buildTestPartition
    val newPartition = partition.map(_.attr * 10)
    var i = 0
    val it = newPartition.iterator
    while (it.hasNext) {
      val edge = it.next()
      assert(edge.srcId == i)
      assert(edge.dstId == i)
      assert(edge.attr == i * 10)
      i += 1
    }
    assert(i == newPartition.size)
  }

  it should "map edge attributes with iterator" in {
    val partition = buildTestPartition
    val newAttributes = (0 until partition.size).map(_ * 10)
    val newPartition = partition.map(newAttributes.iterator)
    var i = 0
    val it = newPartition.iterator
    while (it.hasNext) {
      val edge = it.next()
      assert(edge.srcId == i)
      assert(edge.dstId == i)
      assert(edge.attr == i * 10)
      i += 1
    }
    assert(i == newPartition.size)
  }

  it should "filter edges" in {
    var partition = buildTestPartition
    partition = updatePartitionVertices(partition)
    val newPartition = partition.filter(_.attr % 2 == 0, (id, _) => id % 2 == 0)

    val it = newPartition.iterator
    while (it.hasNext) {
      val edge = it.next()
      assert(edge.srcId % 2 == 0)
      assert(edge.dstId % 2 == 0)
      assert(edge.attr % 2 == 0)
    }
  }

  it should "filter edges with large partition" in {
    val size = 50000
    var partition = buildEdgePartition(2, size)
    partition = updatePartitionVertices(partition)
    val newPartition = partition.filter(_.attr % 2 == 0, (id, _) => id % 2 == 0)

    val it = newPartition.iterator
    while (it.hasNext) {
      val edge = it.next()
      assert(edge.srcId % 2 == 0)
      assert(edge.dstId % 2 == 0)
      assert(edge.attr % 2 == 0)
    }
  }

  it should "inner join with another partition" in {
    val p1 = buildTestPartition
    val p2 = buildTestPartition.map(-_.attr)
    val joinedPartition = p1.innerJoin(p2)((_, _, attr1, attr2) => attr1 + attr2)
    var i = 0
    val it = joinedPartition.iterator
    while (it.hasNext) {
      val edge = it.next()
      assert(edge.srcId == i)
      assert(edge.dstId == i)
      assert(edge.attr == 0)
      i += 1
    }
    assert(i == joinedPartition.size)
  }

  it should "inner join with another non-intersecting partition" in {
    val p1 = buildTestPartition

    val builder = PKEdgePartitionBuilder[Int, Int](2)
    for (i <- 20 until 30) {
      builder.add(i, i, i)
    }

    val p2 = builder.build
    val joinedPartition = p1.innerJoin(p2)((_, _, attr1, attr2) => attr1 + attr2)
    val it = joinedPartition.iterator
    assert(!it.hasNext)
  }

  it should "aggregate messages" in {
    val partition = buildTestPartition
    val it = partition.aggregateMessages[Int](
      context => context.sendToDst(1),
      (a, b) => a + b,
      TripletFields.None,
      EdgeActiveness.Neither
    )

    var i = 0
    while (it.hasNext) {
      val (_, attr) = it.next()
      assert(attr == 1)
      i += 1
    }
    assert(i == partition.size)
  }

  it should "aggregate messages with active set" in {
    val partition = buildTestPartition.withActiveSet((0 to 5).iterator.map(_.toLong))
    val it = partition.aggregateMessages[Int](
      context => context.sendToDst(1),
      _ + _,
      TripletFields.None,
      EdgeActiveness.Both
    )

    var i = 0
    while (it.hasNext) {
      val (_, attr) = it.next()
      assert(attr == 1)
      i += 1
    }

    assert(i - 1 == partition.size / 2)
  }

  private def updatePartitionVertices(partition: PKEdgePartition[Int, Int]): PKEdgePartition[Int, Int] = {
    val vertices = partition.vertexAttrs.indices.map(i => (i.toLong, i * 10))
    partition.updateVertices(vertices.iterator)
  }

  def buildEdgePartition(k: Int, size: Int): PKEdgePartition[Int, Int] = {
    val builder = PKEdgePartitionBuilder[Int, Int](k, size)
    val sqrSize = math.floor(math.sqrt(size)).toInt

    for (i <- 0 until size) {
      val src = i / sqrSize
      val dst = i % sqrSize
      builder.add(src, dst, src + dst)
    }

    builder.build
  }

  private def buildTestPartition: PKEdgePartition[Int, Int] = {
    val builder = PKEdgePartitionBuilder[Int, Int](2)
    for (i <- 0 until 10) {
      builder.add(i, i, i)
    }
    builder.build
  }
}
