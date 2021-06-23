package org.apache.spark.graphx.pkgraph.macrobenchmarks

import org.apache.spark.graphx.pkgraph.macrobenchmarks.algorithms.{ConnectedComponentsAlgorithm, GraphAlgorithm, PageRankAlgorithm, ShortestPathAlgorithm, TriangleCountAlgorithm}
import org.apache.spark.graphx.pkgraph.macrobenchmarks.datasets.{GraphDatasetGenerator, MemoryDatasetGenerator}
import org.apache.spark.graphx.pkgraph.macrobenchmarks.generators.{GraphGenerator, GraphXGenerator, PKGraphGenerator}
import org.apache.spark.graphx.pkgraph.macrobenchmarks.metrics.GraphMetricsCollector
import org.apache.spark.sql.SparkSession

import java.io.PrintStream
import scala.io.StdIn

object GraphBenchmark {
  def getGraphDatasetGeneratorFromArgs(dataset: String): GraphDatasetGenerator = {
    dataset match {
      case "memory" => new MemoryDatasetGenerator()
      case i        => throw new IllegalArgumentException(s"unknown dataset '$i'")
    }
  }

  def getGraphGeneratorFromArgs(implementation: String): GraphGenerator = {
    implementation match {
      case "GraphX"   => new GraphXGenerator()
      case "PKGraph2" => new PKGraphGenerator(2)
      case "PKGraph4" => new PKGraphGenerator(4)
      case "PKGraph8" => new PKGraphGenerator(8)
      case i          => throw new IllegalArgumentException(s"unknown implementation '$i'")
    }
  }

  def getGraphAlgorithmFromArgs(algorithm: String): GraphAlgorithm = {
    algorithm match {
      case "pageRank"            => new PageRankAlgorithm()
      case "triangleCount"       => new TriangleCountAlgorithm()
      case "connectedComponents" => new ConnectedComponentsAlgorithm()
      case "shortestPath"        => new ShortestPathAlgorithm()
      case i                     => throw new IllegalArgumentException(s"unknown algorithm '$i'")
    }
  }

  def main(args: Array[String]): Unit = {
    assert(args.length == 3, "Wrong usage: graph-benchmark <implementation> <algorithm> <dataset>")
    val graph = getGraphGeneratorFromArgs(args(0))
    val algorithm = getGraphAlgorithmFromArgs(args(1))
    val dataset = getGraphDatasetGeneratorFromArgs(args(2))

    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("Graph Benchmark")
      .getOrCreate()

    val sc = spark.sparkContext
    val metricsCollector = new GraphMetricsCollector(sc, args(0), args(1), args(2))
    metricsCollector.start()
    algorithm.run(graph.generate(dataset.dataset(sc)))
    metricsCollector.stop()

    val logs = new PrintStream(s"macrobenchmarks/reports/metrics-${args(0)}-${args(1)}-${args(2)}.txt")
    metricsCollector.printCollectedMetrics(logs)

    println("Press any key to continue...")
    StdIn.readLine()
    spark.stop()
  }
}
