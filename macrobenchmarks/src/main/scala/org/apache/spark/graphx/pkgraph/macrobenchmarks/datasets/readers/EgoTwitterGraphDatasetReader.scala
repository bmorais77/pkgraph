package org.apache.spark.graphx.pkgraph.macrobenchmarks.datasets.readers

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.graphx.pkgraph.macrobenchmarks.datasets.{GraphDataset, GraphDatasetReader}
import org.apache.spark.rdd.RDD

class EgoTwitterGraphDatasetReader extends GraphDatasetReader {
  override def readDataset(sc: SparkContext, path: String): GraphDataset = {
    val edges: RDD[Edge[Int]] = sc
      .textFile(s"$path.txt")
      .map { line =>
        val args = line.split(' ')
        assert(args.length == 2)
        Edge(args(0).toLong, args(1).toLong, 1) // No attribute
      }

    val vertices: RDD[(VertexId, Long)] = edges
      .flatMap(e => Seq((e.srcId, 0L), (e.dstId, 1L))) // No attribute
      .distinct()

    GraphDataset(vertices, edges)
  }
}
