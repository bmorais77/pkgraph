package org.apache.spark.graphx.pkgraph.macrobenchmarks.datasets

import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.rdd.RDD

case class GraphDataset(vertices: RDD[(VertexId, Long)], edges: RDD[Edge[Int]])
