package org.apache.spark.graphx.pkgraph.macrobenchmarks.datasets

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.rdd.RDD

class GraphDatasetReader {
  def readDataset(sc: SparkContext, path: String): GraphDataset = {
    val vertices: RDD[(VertexId, Long)] = sc
      .textFile(s"$path/vertices.txt")
      .map { line =>
        val args = line.split(',')
        assert(args.length == 2)
        (args(0).toLong, args(1).toLong)
      }

    val edges: RDD[Edge[Int]] = sc
      .textFile(s"$path/edges.txt")
      .map { line =>
        val args = line.split(',')
        assert(args.length == 3)
        Edge(args(0).toLong, args(1).toLong, args(2).toInt)
      }

    GraphDataset(vertices, edges)
  }
}
