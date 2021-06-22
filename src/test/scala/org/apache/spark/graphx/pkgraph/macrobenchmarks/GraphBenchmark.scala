package org.apache.spark.graphx.pkgraph.macrobenchmarks

import org.apache.spark.graphx.pkgraph.macrobenchmarks.datasets.{GraphDatasetGenerator, MemoryDatasetGenerator}
import org.apache.spark.graphx.pkgraph.macrobenchmarks.algorithms.{GraphAlgorithm, PageRankAlgorithm}
import org.apache.spark.graphx.pkgraph.macrobenchmarks.generators.{GraphGenerator, GraphXGenerator}
import org.apache.spark.sql.SparkSession

object GraphBenchmark {
  def getGraphDatasetGeneratorFromArgs(dataset: String): GraphDatasetGenerator = {
    dataset match {
      case "memory" => new MemoryDatasetGenerator()
      case i        => throw new IllegalArgumentException(s"unknown dataset '$i'")
    }
  }

  def getGraphGeneratorFromArgs(implementation: String): GraphGenerator = {
    implementation match {
      case "GraphX" => new GraphXGenerator()
      case i        => throw new IllegalArgumentException(s"unknown implementation '$i'")
    }
  }

  def getGraphAlgorithmFromArgs(algorithm: String): GraphAlgorithm = {
    algorithm match {
      case "pageRank" => new PageRankAlgorithm()
      case i          => throw new IllegalArgumentException(s"unknown algorithm '$i'")
    }
  }

  def run(graph: GraphGenerator, algorithm: GraphAlgorithm, dataset: GraphDatasetGenerator): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Graph Benchmark")
      .getOrCreate()

    val sc = spark.sparkContext
    algorithm.run(graph.generate(dataset.dataset(sc)))
    spark.stop()
  }

  def main(args: Array[String]): Unit = {
    assert(args.length == 3, "Wrong usage: graph-benchmark <implementation> <algorithm> <dataset>")
    val graph = getGraphGeneratorFromArgs(args(0))
    val algorithm = getGraphAlgorithmFromArgs(args(1))
    val dataset = getGraphDatasetGeneratorFromArgs(args(2))
    run(graph, algorithm, dataset)
  }
}
