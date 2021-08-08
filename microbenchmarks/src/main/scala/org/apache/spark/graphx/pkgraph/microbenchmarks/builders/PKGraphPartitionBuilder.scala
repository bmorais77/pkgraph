package org.apache.spark.graphx.pkgraph.microbenchmarks.builders

import org.apache.spark.graphx.pkgraph.graph.{PKEdgePartition, PKEdgePartitionBuilder}
import org.apache.spark.ml.linalg.SparseMatrix

import java.util.Random

object PKGraphPartitionBuilder {
  def build(k: Int, size: Int, density: Float, activePercentage: Float): PKEdgePartition[Int, Int] = {
    val matrixSize = math.floor(math.sqrt(size)).toInt
    val sparseMatrix = SparseMatrix.sprand(matrixSize, matrixSize, density, new Random())
    val builder = PKEdgePartitionBuilder[Int, Int](k, sparseMatrix.numActives)

    sparseMatrix.foreachActive { (line, col, attr) =>
      builder.add(line, col, attr.toInt)
    }

    val activeVertices = 0L until (matrixSize * activePercentage).toInt
    val vertices = activeVertices.map(i => (i, i.toInt * 10))

    builder
      .build()
      .updateVertices(vertices.iterator)
      .withActiveSet(activeVertices.iterator)
  }
}
