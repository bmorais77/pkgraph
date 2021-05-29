package org.apache.spark.graphx.pkgraph.benchmarks

import org.apache.spark.graphx.impl.{EdgePartition, EdgePartitionBuilder}
import org.apache.spark.graphx.pkgraph.graph.{PKEdgePartition, PKEdgePartitionBuilder}
import org.apache.spark.ml.linalg.SparseMatrix
import org.scalameter.api._

import java.util.Random

object EdgePartitionDataSet {
  lazy val edges: Gen[Int] = Gen.range("edges")(10000, 100000, 10000)

  /**
    * GraphX Partitions
    */

  lazy val graphX20SparsePartitions: Gen[EdgePartition[Int, Int]] = for { size <- edges } yield {
    buildGraphXEdgePartition(size, 0.2f)
  }

  lazy val graphX20SparsePartitionsWithVertices: Gen[EdgePartition[Int, Int]] = for { size <- edges } yield {
    val sqrSize = math.floor(math.sqrt(size)).toInt
    val vertices = (0 until sqrSize).map(i => (i.toLong, i * 10))
    val partition = buildGraphXEdgePartition(size, 0.2f)
    partition.updateVertices(vertices.iterator)
    partition
  }

  lazy val graphX20SparseInnerJoinPartitions: Gen[(EdgePartition[Int, Int], EdgePartition[Int, Int])] = for {
    partition <- graphX20SparsePartitions
  } yield {
    (partition, partition)
  }

  lazy val graphX40SparsePartitions: Gen[EdgePartition[Int, Int]] = for { size <- edges } yield {
    buildGraphXEdgePartition(size, 0.4f)
  }

  lazy val graphX40SparsePartitionsWithVertices: Gen[EdgePartition[Int, Int]] = for { size <- edges } yield {
    val sqrSize = math.floor(math.sqrt(size)).toInt
    val vertices = (0 until sqrSize).map(i => (i.toLong, i * 10))
    val partition = buildGraphXEdgePartition(size, 0.4f)
    partition.updateVertices(vertices.iterator)
    partition
  }

  lazy val graphX40SparseInnerJoinPartitions: Gen[(EdgePartition[Int, Int], EdgePartition[Int, Int])] = for {
    partition <- graphX40SparsePartitions
  } yield {
    (partition, partition)
  }

  lazy val graphX60SparsePartitions: Gen[EdgePartition[Int, Int]] = for { size <- edges } yield {
    buildGraphXEdgePartition(size, 0.6f)
  }

  lazy val graphX60SparsePartitionsWithVertices: Gen[EdgePartition[Int, Int]] = for { size <- edges } yield {
    val sqrSize = math.floor(math.sqrt(size)).toInt
    val vertices = (0 until sqrSize).map(i => (i.toLong, i * 10))
    val partition = buildGraphXEdgePartition(size, 0.6f)
    partition.updateVertices(vertices.iterator)
    partition
  }

  lazy val graphX60SparseInnerJoinPartitions: Gen[(EdgePartition[Int, Int], EdgePartition[Int, Int])] = for {
    partition <- graphX60SparsePartitions
  } yield {
    (partition, partition)
  }

  lazy val graphX80SparsePartitions: Gen[EdgePartition[Int, Int]] = for { size <- edges } yield {
    buildGraphXEdgePartition(size, 0.8f)
  }

  lazy val graphX80SparsePartitionsWithVertices: Gen[EdgePartition[Int, Int]] = for { size <- edges } yield {
    val sqrSize = math.floor(math.sqrt(size)).toInt
    val vertices = (0 until sqrSize).map(i => (i.toLong, i * 10))
    val partition = buildGraphXEdgePartition(size, 0.8f)
    partition.updateVertices(vertices.iterator)
    partition
  }

  lazy val graphX80SparseInnerJoinPartitions: Gen[(EdgePartition[Int, Int], EdgePartition[Int, Int])] = for {
    partition <- graphX80SparsePartitions
  } yield {
    (partition, partition)
  }

  lazy val graphXFullPartitions: Gen[EdgePartition[Int, Int]] = for { size <- edges } yield {
    buildGraphXEdgePartition(size, 1.0f)
  }

  lazy val graphXFullPartitionsWithVertices: Gen[EdgePartition[Int, Int]] = for { size <- edges } yield {
    val sqrSize = math.floor(math.sqrt(size)).toInt
    val vertices = (0 until sqrSize).map(i => (i.toLong, i * 10))
    val partition = buildGraphXEdgePartition(size, 1.0f)
    partition.updateVertices(vertices.iterator)
    partition
  }

  lazy val graphXFullInnerJoinPartitions: Gen[(EdgePartition[Int, Int], EdgePartition[Int, Int])] = for {
    partition <- graphXFullPartitions
  } yield {
    (partition, partition)
  }

  /**
    * PKGraph Partitions (k = 2)
    */

  lazy val k2PKGraph20SparsePartitions: Gen[PKEdgePartition[Int, Int]] = for { size <- edges } yield {
    buildPKGraphEdgePartition(2, size, 0.2f)
  }

  lazy val k2PKGraph20SparsePartitionsWithVertices: Gen[PKEdgePartition[Int, Int]] = for {
    size <- edges
  } yield {
    val partition = buildPKGraphEdgePartition(2, size, 0.2f)
    partition.updateVertices((0 until partition.tree.size).map(i => (i.toLong, i * 10)).iterator)
  }

  lazy val k2PKGraph20SparseInnerJoinPartitions: Gen[(PKEdgePartition[Int, Int], PKEdgePartition[Int, Int])] = for {
    partition <- k2PKGraph20SparsePartitions
  } yield {
    (partition, partition)
  }

  lazy val k2PKGraph40SparsePartitions: Gen[PKEdgePartition[Int, Int]] = for { size <- edges } yield {
    buildPKGraphEdgePartition(2, size, 0.4f)
  }

  lazy val k2PKGraph40SparsePartitionsWithVertices: Gen[PKEdgePartition[Int, Int]] = for {
    size <- edges
  } yield {
    val partition = buildPKGraphEdgePartition(2, size, 0.4f)
    partition.updateVertices((0 until partition.tree.size).map(i => (i.toLong, i * 10)).iterator)
  }

  lazy val k2PKGraph40SparseInnerJoinPartitions: Gen[(PKEdgePartition[Int, Int], PKEdgePartition[Int, Int])] = for {
    partition <- k2PKGraph40SparsePartitions
  } yield {
    (partition, partition)
  }

  lazy val k2PKGraph60SparsePartitions: Gen[PKEdgePartition[Int, Int]] = for { size <- edges } yield {
    buildPKGraphEdgePartition(2, size, 0.6f)
  }

  lazy val k2PKGraph60SparsePartitionsWithVertices: Gen[PKEdgePartition[Int, Int]] = for {
    size <- edges
  } yield {
    val partition = buildPKGraphEdgePartition(2, size, 0.6f)
    partition.updateVertices((0 until partition.tree.size).map(i => (i.toLong, i * 10)).iterator)
  }

  lazy val k2PKGraph60SparseInnerJoinPartitions: Gen[(PKEdgePartition[Int, Int], PKEdgePartition[Int, Int])] = for {
    partition <- k2PKGraph60SparsePartitions
  } yield {
    (partition, partition)
  }

  lazy val k2PKGraph80SparsePartitions: Gen[PKEdgePartition[Int, Int]] = for { size <- edges } yield {
    buildPKGraphEdgePartition(2, size, 0.8f)
  }

  lazy val k2PKGraph80SparsePartitionsWithVertices: Gen[PKEdgePartition[Int, Int]] = for {
    size <- edges
  } yield {
    val partition = buildPKGraphEdgePartition(2, size, 0.8f)
    partition.updateVertices((0 until partition.tree.size).map(i => (i.toLong, i * 10)).iterator)
  }

  lazy val k2PKGraph80SparseInnerJoinPartitions: Gen[(PKEdgePartition[Int, Int], PKEdgePartition[Int, Int])] = for {
    partition <- k2PKGraph80SparsePartitions
  } yield {
    (partition, partition)
  }

  lazy val k2PKGraphFullPartitions: Gen[PKEdgePartition[Int, Int]] = for { size <- edges } yield {
    buildPKGraphEdgePartition(2, size, 1f)
  }

  lazy val k2PKGraphFullPartitionsWithVertices: Gen[PKEdgePartition[Int, Int]] = for { size <- edges } yield {
    val partition = buildPKGraphEdgePartition(2, size, 1f)
    partition.updateVertices((0 until partition.tree.size).map(i => (i.toLong, i * 10)).iterator)
  }

  lazy val k2PKGraphFullInnerJoinPartitions: Gen[(PKEdgePartition[Int, Int], PKEdgePartition[Int, Int])] = for {
    partition <- k2PKGraphFullPartitions
  } yield {
    (partition, partition)
  }

  /**
    * PKGraph Partitions (k = 4)
    */

  lazy val k4PKGraph20SparsePartitions: Gen[PKEdgePartition[Int, Int]] = for { size <- edges } yield {
    buildPKGraphEdgePartition(4, size, 0.2f)
  }

  lazy val k4PKGraph20SparsePartitionsWithVertices: Gen[PKEdgePartition[Int, Int]] = for {
    size <- edges
  } yield {
    val partition = buildPKGraphEdgePartition(4, size, 0.2f)
    partition.updateVertices((0 until partition.tree.size).map(i => (i.toLong, i * 10)).iterator)
  }

  lazy val k4PKGraph20SparseInnerJoinPartitions: Gen[(PKEdgePartition[Int, Int], PKEdgePartition[Int, Int])] = for {
    partition <- k4PKGraph20SparsePartitions
  } yield {
    (partition, partition)
  }

  lazy val k4PKGraph40SparsePartitions: Gen[PKEdgePartition[Int, Int]] = for { size <- edges } yield {
    buildPKGraphEdgePartition(4, size, 0.4f)
  }

  lazy val k4PKGraph40SparsePartitionsWithVertices: Gen[PKEdgePartition[Int, Int]] = for {
    size <- edges
  } yield {
    val partition = buildPKGraphEdgePartition(4, size, 0.4f)
    partition.updateVertices((0 until partition.tree.size).map(i => (i.toLong, i * 10)).iterator)
  }

  lazy val k4PKGraph40SparseInnerJoinPartitions: Gen[(PKEdgePartition[Int, Int], PKEdgePartition[Int, Int])] = for {
    partition <- k4PKGraph40SparsePartitions
  } yield {
    (partition, partition)
  }

  lazy val k4PKGraph60SparsePartitions: Gen[PKEdgePartition[Int, Int]] = for { size <- edges } yield {
    buildPKGraphEdgePartition(4, size, 0.6f)
  }

  lazy val k4PKGraph60SparsePartitionsWithVertices: Gen[PKEdgePartition[Int, Int]] = for {
    size <- edges
  } yield {
    val partition = buildPKGraphEdgePartition(4, size, 0.6f)
    partition.updateVertices((0 until partition.tree.size).map(i => (i.toLong, i * 10)).iterator)
  }

  lazy val k4PKGraph60SparseInnerJoinPartitions: Gen[(PKEdgePartition[Int, Int], PKEdgePartition[Int, Int])] = for {
    partition <- k4PKGraph60SparsePartitions
  } yield {
    (partition, partition)
  }

  lazy val k4PKGraph80SparsePartitions: Gen[PKEdgePartition[Int, Int]] = for { size <- edges } yield {
    buildPKGraphEdgePartition(4, size, 0.8f)
  }

  lazy val k4PKGraph80SparsePartitionsWithVertices: Gen[PKEdgePartition[Int, Int]] = for {
    size <- edges
  } yield {
    val partition = buildPKGraphEdgePartition(4, size, 0.8f)
    partition.updateVertices((0 until partition.tree.size).map(i => (i.toLong, i * 10)).iterator)
  }

  lazy val k4PKGraph80SparseInnerJoinPartitions: Gen[(PKEdgePartition[Int, Int], PKEdgePartition[Int, Int])] = for {
    partition <- k4PKGraph80SparsePartitions
  } yield {
    (partition, partition)
  }

  lazy val k4PKGraphFullPartitions: Gen[PKEdgePartition[Int, Int]] = for { size <- edges } yield {
    buildPKGraphEdgePartition(4, size, 1f)
  }

  lazy val k4PKGraphFullPartitionsWithVertices: Gen[PKEdgePartition[Int, Int]] = for { size <- edges } yield {
    val partition = buildPKGraphEdgePartition(4, size, 1f)
    partition.updateVertices((0 until partition.tree.size).map(i => (i.toLong, i * 10)).iterator)
  }

  lazy val k4PKGraphFullInnerJoinPartitions: Gen[(PKEdgePartition[Int, Int], PKEdgePartition[Int, Int])] = for {
    partition <- k4PKGraphFullPartitions
  } yield {
    (partition, partition)
  }

  /**
    * PKGraph Partitions (k = 8)
    */

  lazy val k8PKGraph20SparsePartitions: Gen[PKEdgePartition[Int, Int]] = for { size <- edges } yield {
    buildPKGraphEdgePartition(8, size, 0.2f)
  }

  lazy val k8PKGraph20SparsePartitionsWithVertices: Gen[PKEdgePartition[Int, Int]] = for {
    size <- edges
  } yield {
    val partition = buildPKGraphEdgePartition(8, size, 0.2f)
    partition.updateVertices((0 until partition.tree.size).map(i => (i.toLong, i * 10)).iterator)
  }

  lazy val k8PKGraph20SparseInnerJoinPartitions: Gen[(PKEdgePartition[Int, Int], PKEdgePartition[Int, Int])] = for {
    partition <- k8PKGraph20SparsePartitions
  } yield {
    (partition, partition)
  }

  lazy val k8PKGraph40SparsePartitions: Gen[PKEdgePartition[Int, Int]] = for { size <- edges } yield {
    buildPKGraphEdgePartition(8, size, 0.4f)
  }

  lazy val k8PKGraph40SparsePartitionsWithVertices: Gen[PKEdgePartition[Int, Int]] = for {
    size <- edges
  } yield {
    val partition = buildPKGraphEdgePartition(8, size, 0.4f)
    partition.updateVertices((0 until partition.tree.size).map(i => (i.toLong, i * 10)).iterator)
  }

  lazy val k8PKGraph40SparseInnerJoinPartitions: Gen[(PKEdgePartition[Int, Int], PKEdgePartition[Int, Int])] = for {
    partition <- k8PKGraph40SparsePartitions
  } yield {
    (partition, partition)
  }

  lazy val k8PKGraph60SparsePartitions: Gen[PKEdgePartition[Int, Int]] = for { size <- edges } yield {
    buildPKGraphEdgePartition(8, size, 0.6f)
  }

  lazy val k8PKGraph60SparsePartitionsWithVertices: Gen[PKEdgePartition[Int, Int]] = for {
    size <- edges
  } yield {
    val partition = buildPKGraphEdgePartition(8, size, 0.6f)
    partition.updateVertices((0 until partition.tree.size).map(i => (i.toLong, i * 10)).iterator)
  }

  lazy val k8PKGraph60SparseInnerJoinPartitions: Gen[(PKEdgePartition[Int, Int], PKEdgePartition[Int, Int])] = for {
    partition <- k8PKGraph60SparsePartitions
  } yield {
    (partition, partition)
  }

  lazy val k8PKGraph80SparsePartitions: Gen[PKEdgePartition[Int, Int]] = for { size <- edges } yield {
    buildPKGraphEdgePartition(8, size, 0.8f)
  }

  lazy val k8PKGraph80SparsePartitionsWithVertices: Gen[PKEdgePartition[Int, Int]] = for {
    size <- edges
  } yield {
    val partition = buildPKGraphEdgePartition(8, size, 0.8f)
    partition.updateVertices((0 until partition.tree.size).map(i => (i.toLong, i * 10)).iterator)
  }

  lazy val k8PKGraph80SparseInnerJoinPartitions: Gen[(PKEdgePartition[Int, Int], PKEdgePartition[Int, Int])] = for {
    partition <- k8PKGraph80SparsePartitions
  } yield {
    (partition, partition)
  }

  lazy val k8PKGraphFullPartitions: Gen[PKEdgePartition[Int, Int]] = for { size <- edges } yield {
    buildPKGraphEdgePartition(8, size, 1f)
  }

  lazy val k8PKGraphFullPartitionsWithVertices: Gen[PKEdgePartition[Int, Int]] = for { size <- edges } yield {
    val partition = buildPKGraphEdgePartition(8, size, 1f)
    partition.updateVertices((0 until partition.tree.size).map(i => (i.toLong, i * 10)).iterator)
  }

  lazy val k8PKGraphFullInnerJoinPartitions: Gen[(PKEdgePartition[Int, Int], PKEdgePartition[Int, Int])] = for {
    partition <- k8PKGraphFullPartitions
  } yield {
    (partition, partition)
  }

  /**
    * Builds an GraphX Edge partition.
    *
    * @param size           Size of the partition (i.e maximum number of edges the partition can hold)
    * @param sparsity       Percentage of the partition that does not contain any edge
    * @return edge partition
    */
  def buildGraphXEdgePartition(size: Int, sparsity: Float): EdgePartition[Int, Int] = {
    val builder = new EdgePartitionBuilder[Int, Int](size)
    val matrixSize = math.floor(math.sqrt(size)).toInt
    val sparseMatrix = SparseMatrix.sprand(matrixSize, matrixSize, sparsity, new Random())

    sparseMatrix.foreachActive { (line, col, attr) =>
      builder.add(line, col, attr.toInt)
    }

    builder.toEdgePartition
  }

  /**
    * Builds an PKGraph Edge partition.
    *
    * @param k              Value of the K²-Tree
    * @param size           Size of the partition (i.e maximum number of edges the partition can hold)
    * @param sparsity       Percentage of the partition that does not contain any edge
    * @return edge partition
    */
  def buildPKGraphEdgePartition(k: Int, size: Int, sparsity: Float): PKEdgePartition[Int, Int] = {
    val matrixSize = math.floor(math.sqrt(size)).toInt
    val builder = PKEdgePartitionBuilder[Int, Int](k, size)
    val sparseMatrix = SparseMatrix.sprand(matrixSize, matrixSize, 1.0f - sparsity, new Random())

    sparseMatrix.foreachActive { (line, col, attr) =>
      builder.add(line, col, attr.toInt)
    }

    builder.build
  }
}
