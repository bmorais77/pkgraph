package org.apache.spark.graphx.pkgraph.macrobenchmarks.datasets

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.rdd.RDD

class MTXGraphDatasetReader {
  def readDataset(sc: SparkContext, path: String): GraphDataset = {
    val edges: RDD[Edge[Int]] = sc
      .textFile(s"$path.mtx")
      .filter(line => !line.startsWith("%") && !line.startsWith("#")) // Skip comments
      .mapPartitionsWithIndex(
        (idx, it) => if (idx == 0) it.drop(1) else it,
        preservesPartitioning = true
      ) // Skip header
      .map { line =>
        val args = line.split("\\s+")
        assert(args.length >= 2, s"line '$line'")
        Edge(args(0).toLong, args(1).toLong, if (args.length > 2) args(2).toInt else 0)
      }
      .cache()

    val vertices: RDD[(VertexId, Long)] = edges
      .flatMap(e => Seq((e.srcId, 1L), (e.dstId, 1L))) // No attribute
      .distinct()
      .cache()

    GraphDataset(vertices, edges)
  }
}
