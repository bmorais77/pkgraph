package org.apache.spark.graphx.pkgraph.microbenchmarks.datasets

import org.apache.spark.graphx.pkgraph.graph.{PKEdgePartition, PKEdgePartitionBuilder}
import org.apache.spark.ml.linalg.SparseMatrix
import org.scalameter.api.Gen

import java.util.Random

object PKGraphDataSet {

  def buildPartitions(k: Int, sparsity: Float): Gen[PKEdgePartition[Int, Int]] = {
    for { size <- EdgesDataSet.edges } yield {
      buildPKGraphEdgePartition(k, size, sparsity)
    }
  }

  def buildPartitionsWithVertices(k: Int, sparsity: Float): Gen[PKEdgePartition[Int, Int]] = {
    for { partition <- buildPartitions(k, sparsity) } yield {
      val vertices = partition.vertexAttrs.indices.map(i => (i.toLong, i * 10))
      partition.updateVertices(vertices.iterator)
    }
  }

  def buildPartitionsForInnerJoin(
      k: Int,
      sparsity: Float
  ): Gen[(PKEdgePartition[Int, Int], PKEdgePartition[Int, Int])] = {
    for { partition <- buildPartitions(k, sparsity) } yield {
      (partition, partition)
    }
  }

  def buildPartitionsWithActiveVertices(k: Int, sparsity: Float): Gen[PKEdgePartition[Int, Int]] = {
    for { partition <- buildPartitionsWithVertices(k, sparsity) } yield {
      val sqrSize = math.floor(math.sqrt(partition.size)).toInt
      val activeVertices = (0L until sqrSize.toLong).filter(i => i % 10 == 0).iterator
      partition.withActiveSet(activeVertices)
    }
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
    val sparseMatrix = SparseMatrix.sprand(matrixSize, matrixSize, 1.0f - sparsity, new Random())
    val builder = PKEdgePartitionBuilder[Int, Int](k, sparseMatrix.numActives)

    sparseMatrix.foreachActive { (line, col, attr) =>
      builder.add(line, col, attr.toInt)
    }

    builder.build()
  }
}
