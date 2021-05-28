package org.apache.spark.graphx.pkgraph.benchmarks

import org.apache.spark.graphx.impl.{EdgePartition, EdgePartitionBuilder}
import org.apache.spark.graphx.pkgraph.graph.{PKEdgePartition, PKEdgePartitionBuilder}
import org.scalameter.api._

object EdgePartitionDataSet {
  lazy val partitionSizes: Gen[Int] = Gen.range("partition_size")(10000, 100000, 10000)

  /**
    * GraphX Partitions
    */

  lazy val graphX20SparsePartitions: Gen[EdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    buildGraphXEdgePartition(size, 0.2f)
  }

  lazy val graphX20SparsePartitionsWithVertices: Gen[EdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    val sqrSize = math.floor(math.sqrt(size)).toInt
    val vertices = (0 until sqrSize).map(i => (i.toLong, i * 10))
    val partition = buildGraphXEdgePartition(size, 0.2f)
    partition.updateVertices(vertices.iterator)
    partition
  }

  lazy val graphX40SparsePartitions: Gen[EdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    buildGraphXEdgePartition(size, 0.4f)
  }

  lazy val graphX40SparsePartitionsWithVertices: Gen[EdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    val sqrSize = math.floor(math.sqrt(size)).toInt
    val vertices = (0 until sqrSize).map(i => (i.toLong, i * 10))
    val partition = buildGraphXEdgePartition(size, 0.4f)
    partition.updateVertices(vertices.iterator)
    partition
  }

  lazy val graphX60SparsePartitions: Gen[EdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    buildGraphXEdgePartition(size, 0.6f)
  }

  lazy val graphX60SparsePartitionsWithVertices: Gen[EdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    val sqrSize = math.floor(math.sqrt(size)).toInt
    val vertices = (0 until sqrSize).map(i => (i.toLong, i * 10))
    val partition = buildGraphXEdgePartition(size, 0.6f)
    partition.updateVertices(vertices.iterator)
    partition
  }

  lazy val graphX80SparsePartitions: Gen[EdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    buildGraphXEdgePartition(size, 0.8f)
  }

  lazy val graphX80SparsePartitionsWithVertices: Gen[EdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    val sqrSize = math.floor(math.sqrt(size)).toInt
    val vertices = (0 until sqrSize).map(i => (i.toLong, i * 10))
    val partition = buildGraphXEdgePartition(size, 0.8f)
    partition.updateVertices(vertices.iterator)
    partition
  }

  lazy val graphXFullPartitions: Gen[EdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    buildGraphXEdgePartition(size, 1f)
  }

  lazy val graphXFullPartitionsWithVertices: Gen[EdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    val sqrSize = math.floor(math.sqrt(size)).toInt
    val vertices = (0 until sqrSize).map(i => (i.toLong, i * 10))
    val partition = buildGraphXEdgePartition(size, 1f)
    partition.updateVertices(vertices.iterator)
    partition
  }

  /**
    * PKGraph Partitions (k = 2)
    */

  lazy val k2PKGraph20SparsePartitions: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    buildPKGraphEdgePartition(2, size, 0.2f)
  }

  lazy val k2PKGraph20SparsePartitionsWithVertices: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    val partition = buildPKGraphEdgePartition(2, size, 0.2f)
    partition.updateVertices((0 until partition.tree.size).map(i => (i.toLong, i * 10)).iterator)
  }

  lazy val k2PKGraph40SparsePartitions: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    buildPKGraphEdgePartition(2, size, 0.4f)
  }

  lazy val k2PKGraph40SparsePartitionsWithVertices: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    val partition = buildPKGraphEdgePartition(2, size, 0.4f)
    partition.updateVertices((0 until partition.tree.size).map(i => (i.toLong, i * 10)).iterator)
  }

  lazy val k2PKGraph60SparsePartitions: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    buildPKGraphEdgePartition(2, size, 0.6f)
  }

  lazy val k2PKGraph60SparsePartitionsWithVertices: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    val partition = buildPKGraphEdgePartition(2, size, 0.6f)
    partition.updateVertices((0 until partition.tree.size).map(i => (i.toLong, i * 10)).iterator)
  }

  lazy val k2PKGraph80SparsePartitions: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    buildPKGraphEdgePartition(2, size, 0.8f)
  }

  lazy val k2PKGraph80SparsePartitionsWithVertices: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    val partition = buildPKGraphEdgePartition(2, size, 0.8f)
    partition.updateVertices((0 until partition.tree.size).map(i => (i.toLong, i * 10)).iterator)
  }

  lazy val k2PKGraphFullPartitions: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    buildPKGraphEdgePartition(2, size, 1f)
  }

  lazy val k2PKGraphFullPartitionsWithVertices: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    val partition = buildPKGraphEdgePartition(2, size, 1f)
    partition.updateVertices((0 until partition.tree.size).map(i => (i.toLong, i * 10)).iterator)
  }

  /**
    * PKGraph Partitions (k = 4)
    */

  lazy val k4PKGraph20SparsePartitions: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    buildPKGraphEdgePartition(4, size, 0.2f)
  }

  lazy val k4PKGraph20SparsePartitionsWithVertices: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    val partition = buildPKGraphEdgePartition(4, size, 0.2f)
    partition.updateVertices((0 until partition.tree.size).map(i => (i.toLong, i * 10)).iterator)
  }

  lazy val k4PKGraph40SparsePartitions: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    buildPKGraphEdgePartition(4, size, 0.4f)
  }

  lazy val k4PKGraph40SparsePartitionsWithVertices: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    val partition = buildPKGraphEdgePartition(4, size, 0.4f)
    partition.updateVertices((0 until partition.tree.size).map(i => (i.toLong, i * 10)).iterator)
  }

  lazy val k4PKGraph60SparsePartitions: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    buildPKGraphEdgePartition(4, size, 0.6f)
  }

  lazy val k4PKGraph60SparsePartitionsWithVertices: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    val partition = buildPKGraphEdgePartition(4, size, 0.6f)
    partition.updateVertices((0 until partition.tree.size).map(i => (i.toLong, i * 10)).iterator)
  }

  lazy val k4PKGraph80SparsePartitions: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    buildPKGraphEdgePartition(4, size, 0.8f)
  }

  lazy val k4PKGraph80SparsePartitionsWithVertices: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    val partition = buildPKGraphEdgePartition(4, size, 0.8f)
    partition.updateVertices((0 until partition.tree.size).map(i => (i.toLong, i * 10)).iterator)
  }

  lazy val k4PKGraphFullPartitions: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    buildPKGraphEdgePartition(4, size, 1f)
  }

  lazy val k4PKGraphFullPartitionsWithVertices: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    val partition = buildPKGraphEdgePartition(4, size, 1f)
    partition.updateVertices((0 until partition.tree.size).map(i => (i.toLong, i * 10)).iterator)
  }

  /**
    * PKGraph Partitions (k = 8)
    */

  lazy val k8PKGraph20SparsePartitions: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    buildPKGraphEdgePartition(8, size, 0.2f)
  }

  lazy val k8PKGraph20SparsePartitionsWithVertices: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    val partition = buildPKGraphEdgePartition(8, size, 0.2f)
    partition.updateVertices((0 until partition.tree.size).map(i => (i.toLong, i * 10)).iterator)
  }

  lazy val k8PKGraph40SparsePartitions: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    buildPKGraphEdgePartition(8, size, 0.4f)
  }

  lazy val k8PKGraph40SparsePartitionsWithVertices: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    val partition = buildPKGraphEdgePartition(8, size, 0.4f)
    partition.updateVertices((0 until partition.tree.size).map(i => (i.toLong, i * 10)).iterator)
  }

  lazy val k8PKGraph60SparsePartitions: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    buildPKGraphEdgePartition(8, size, 0.6f)
  }

  lazy val k8PKGraph60SparsePartitionsWithVertices: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    val partition = buildPKGraphEdgePartition(8, size, 0.6f)
    partition.updateVertices((0 until partition.tree.size).map(i => (i.toLong, i * 10)).iterator)
  }

  lazy val k8PKGraph80SparsePartitions: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    buildPKGraphEdgePartition(8, size, 0.8f)
  }

  lazy val k8PKGraph80SparsePartitionsWithVertices: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    val partition = buildPKGraphEdgePartition(8, size, 0.8f)
    partition.updateVertices((0 until partition.tree.size).map(i => (i.toLong, i * 10)).iterator)
  }

  lazy val k8PKGraphFullPartitions: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    buildPKGraphEdgePartition(8, size, 1f)
  }

  lazy val k8PKGraphFullPartitionsWithVertices: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    val partition = buildPKGraphEdgePartition(8, size, 1f)
    partition.updateVertices((0 until partition.tree.size).map(i => (i.toLong, i * 10)).iterator)
  }

  /**
    * Builds an GraphX Edge partition.
    *
    * @param size          Capacity of the partition in edges
    * @param percentage    Percentage of the total size of the partition that contains edges
    * @return sparse edge partition
    */
  private def buildGraphXEdgePartition(size: Int, percentage: Float): EdgePartition[Int, Int] = {
    val builder = new EdgePartitionBuilder[Int, Int](size)
    val sqrSize = math.floor(math.sqrt(size)).toInt

    for (i <- 0 until (size * percentage).toInt) {
      val src = i / sqrSize
      val dst = i % sqrSize
      builder.add(src, dst, src + dst)
    }

    builder.toEdgePartition
  }

  /**
    * Builds an PKGraph Edge partition.
    *
    * @param k             Value of the K²-Tree
    * @param size          Capacity of the partition in edges
    * @param percentage    Percentage of the total size of the partition that contains edges
    * @return sparse edge partition
    */
  private def buildPKGraphEdgePartition(k: Int, size: Int, percentage: Float): PKEdgePartition[Int, Int] = {
    val builder = PKEdgePartitionBuilder[Int, Int](k, size)
    val sqrSize = math.floor(math.sqrt(size)).toInt

    for (i <- 0 until (size * percentage).toInt) {
      val src = i / sqrSize
      val dst = i % sqrSize
      builder.add(src, dst, src + dst)
    }

    builder.build
  }
}
