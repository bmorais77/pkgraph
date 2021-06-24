package org.apache.spark.graphx.pkgraph.macrobenchmarks.datasets

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.ml.linalg.SparseMatrix

import java.util.Random
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class MemoryDatasetGenerator extends GraphDatasetGenerator {
  override def dataset(sc: SparkContext): GraphDataset = {
    val matrix = SparseMatrix.sprand(10000, 10000, 0.01, new Random())
    val vertices = new mutable.HashSet[VertexId]
    val edges = new ArrayBuffer[Edge[Int]]()

    matrix.foreachActive { (line, col, value) =>
      vertices += line
      vertices += col
      edges += Edge(line, col, value.toInt)
    }

    println("---------- [DATASET] ----------")
    println(s"Vertices: ${vertices.size}")
    println(s"Edges: ${edges.length}")
    println("---------- [DATASET] ----------")

    val vertexRdd = sc.parallelize(vertices.toSeq).map(i => (i, i * 10))
    val edgeRdd = sc.parallelize(edges)
    GraphDataset(vertexRdd, edgeRdd)
  }
}
