package org.apache.spark.graphx.pkgraph.macrobenchmarks

import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.ml.linalg.SparseMatrix

import java.io.{File, PrintWriter}
import java.util.Random
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object GraphDatasetGenerator {
  def createOutputDirectory(outputDir: String): Unit = {
    val directory = new File(outputDir)
    if(!directory.exists()) {
      directory.mkdir()
    }
  }

  def main(args: Array[String]): Unit = {
    assert(args.length == 3, "Wrong usage: graph-dataset-generator <size> <density> <output-dir>")
    val size = args(0).toInt
    val density = math.max(0, math.min(args(1).toInt, 100))
    val densityPercentage = density / 100.0

    val outputDir = s"${args(2)}/graph-dataset-$size-$density-generated"
    createOutputDirectory(s"$outputDir")

    println(s"Generating ${size}x$size sparse matrix with $densityPercentage density...")
    val matrix = SparseMatrix.sprand(size, size, densityPercentage, new Random())
    val vertices = new mutable.HashSet[VertexId]
    val edges = new ArrayBuffer[Edge[Int]](size * size)
    matrix.foreachActive { (line, col, attr) =>
      vertices += line
      vertices += col
      edges += Edge(line.toLong, col.toLong, attr.toInt)
    }

    val vertexOutput = s"$outputDir/vertices.txt"
    println(s"Writing ${vertices.size} vertices to $vertexOutput")
    val vertexPrinter = new PrintWriter(vertexOutput)
    for (vertex <- vertices) {
      vertexPrinter.println(s"$vertex,${vertex * 10L}")
    }
    vertexPrinter.close()

    val edgeOutput = s"$outputDir/edges.txt"
    println(s"Writing ${edges.length} edges to $edgeOutput")
    val edgePrinter = new PrintWriter(edgeOutput)
    for (edge <- edges) {
      edgePrinter.println(s"${edge.srcId},${edge.dstId},${edge.attr}")
    }
    edgePrinter.close()
  }
}
