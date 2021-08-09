package org.apache.spark.graphx.pkgraph.macrobenchmarks

import org.apache.spark.ml.linalg.SparseMatrix

import java.io.{File, PrintWriter}
import java.util.Random

object GraphDatasetGenerator {
  def createOutputDirectory(outputDir: String): Unit = {
    val directory = new File(outputDir)
    if (!directory.exists()) {
      directory.mkdir()
    }
  }

  def main(args: Array[String]): Unit = {
    assert(args.length == 3, "Wrong usage: graph-dataset-generator <size> <density> <output>")
    val size = args(0).toInt
    val density = math.max(0, math.min(args(1).toInt, 100))
    val densityPercentage = density / 100.0

    val outputDir = args(2)
    createOutputDirectory(outputDir)

    val output = s"$outputDir/generated-$size-$density.mtx"
    println(s"Output = $output")

    val printer = new PrintWriter(output)

    println(s"Generating ${size}x$size sparse matrix with $densityPercentage density...")
    val matrix = SparseMatrix.sprand(size, size, densityPercentage, new Random())
    matrix.foreachActive { (line, col, attr) =>
      printer.println(s"${line.toLong} ${col.toLong} ${attr.toInt}")
    }

    println("[DONE]")
    printer.close()
  }
}
