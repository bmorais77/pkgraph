package org.apache.spark.graphx.pkgraph.microbenchmarks.builders

import org.apache.spark.graphx.impl.{EdgePartition, EdgePartitionBuilder}
import org.apache.spark.ml.linalg.SparseMatrix

import java.util.Random

object GraphXPartitionBuilder {
  def build(size: Int, density: Float, activePercentage: Float): EdgePartition[Int, Int] = {
    val builder = new EdgePartitionBuilder[Int, Int](size)
    val matrixSize = math.floor(math.sqrt(size)).toInt
    val sparseMatrix = SparseMatrix.sprand(matrixSize, matrixSize, density, new Random())

    sparseMatrix.foreachActive { (line, col, attr) =>
      builder.add(line, col, attr.toInt)
    }

    val activeVertices = 0L until (matrixSize * activePercentage).toInt
    val vertices = activeVertices.map(i => (i, i.toInt * 10))

    builder.toEdgePartition
      .updateVertices(vertices.iterator)
      .withActiveSet(activeVertices.iterator)
  }
}
