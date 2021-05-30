package org.apache.spark.graphx.pkgraph.benchmarks.datasets

import org.apache.spark.graphx.impl.{EdgePartition, EdgePartitionBuilder}
import org.apache.spark.ml.linalg.SparseMatrix
import org.scalameter.api.Gen

import java.util.Random

object GraphXDataSet {
  def buildPartitions(sparsity: Float): Gen[EdgePartition[Int, Int]] = {
    for { size <- EdgesDataSet.edges} yield {
      buildGraphXEdgePartition(size, sparsity)
    }
  }

  def buildPartitionsWithVertices(sparsity: Float): Gen[EdgePartition[Int, Int]] = {
    for { size <- EdgesDataSet.edges} yield {
      val sqrSize = math.floor(math.sqrt(size)).toInt
      val vertices = (0 until sqrSize).map(i => (i.toLong, i * 10))
      val partition = buildGraphXEdgePartition(size, sparsity)
      partition.updateVertices(vertices.iterator)
    }
  }

  def buildPartitionsForInnerJoin(sparsity: Float): Gen[(EdgePartition[Int, Int], EdgePartition[Int, Int])] = {
    for { partition <- buildPartitions(sparsity) } yield {
      (partition, partition)
    }
  }

  def buildPartitionsWithActiveVertices(sparsity: Float): Gen[EdgePartition[Int, Int]] = {
    for { partition <- buildPartitionsWithVertices(sparsity) } yield {
      val sqrSize = math.floor(math.sqrt(partition.size)).toInt
      val activeVertices = (0L until sqrSize.toLong).filter(i => i % 10 == 0).iterator
      partition.withActiveSet(activeVertices)
    }
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
    val sparseMatrix = SparseMatrix.sprand(matrixSize, matrixSize, 1.0f - sparsity, new Random())

    sparseMatrix.foreachActive { (line, col, attr) =>
      builder.add(line, col, attr.toInt)
    }

    builder.toEdgePartition
  }
}
