package org.apache.spark.graphx.pkgraph.macrobenchmarks.datasets.readers

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.graphx.pkgraph.macrobenchmarks.datasets.{GraphDataset, GraphDatasetReader}
import org.apache.spark.rdd.RDD

class MTXGraphDatasetReader extends GraphDatasetReader {
  override def readDataset(sc: SparkContext, path: String): GraphDataset = {
    val edges: RDD[Edge[Int]] = sc
      .textFile(s"$path.mtx")
      .filter(line => !line.startsWith("%")) // Skip comments
      .map { line =>
        val args = line.split(' ')
        assert(args.length == 2)
        Edge(args(0).toLong, args(1).toLong, 1) // No attribute
      }
      .cache()

    val vertices: RDD[(VertexId, Long)] = edges
      .flatMap(e => Seq((e.srcId, 1L), (e.dstId, 1L))) // No attribute
      .distinct()
      .cache()

    GraphDataset(vertices, edges)
  }
}
