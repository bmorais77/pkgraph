package org.apache.spark.graphx.pkgraph.macrobenchmarks

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.pkgraph.macrobenchmarks.algorithms.{
  ConnectedComponentsAlgorithm,
  GraphAlgorithm,
  PageRankAlgorithm,
  ShortestPathAlgorithm,
  TriangleCountAlgorithm
}
import org.apache.spark.graphx.pkgraph.macrobenchmarks.datasets.GraphDatasetReader
import org.apache.spark.graphx.pkgraph.macrobenchmarks.generators.{GraphGenerator, GraphXGenerator, PKGraphGenerator}
import org.apache.spark.graphx.pkgraph.macrobenchmarks.metrics.GraphMetricsCollector
import org.apache.spark.sql.SparkSession

import java.io.PrintStream
import scala.io.StdIn

object GraphBenchmark {
  def getGraphGeneratorFromArgs(implementation: String): GraphGenerator = {
    implementation match {
      case "GraphX"    => new GraphXGenerator()
      case "PKGraph2"  => new PKGraphGenerator(2)
      case "PKGraph4"  => new PKGraphGenerator(4)
      case "PKGraph8"  => new PKGraphGenerator(8)
      case "PKGraph16" => new PKGraphGenerator(16)
      case i           => throw new IllegalArgumentException(s"unknown implementation '$i'")
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
    val implementation = args(0)
    val graphAlgorithm = args(1)
    val graphDataset = args(2)

    val graph = getGraphGeneratorFromArgs(implementation)
    val algorithm = getGraphAlgorithmFromArgs(graphAlgorithm)
    val reader = new GraphDatasetReader

    val config = new SparkConf()
      .setMaster("local[4]")
      .setAppName(s"Graph Benchmark ($implementation | $graphAlgorithm | $graphDataset)")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", "/tmp/spark-events")

    val sc = new SparkContext(config)
    val metricsCollector = new GraphMetricsCollector(sc, implementation, graphAlgorithm, graphDataset)
    metricsCollector.start()

    val datasetPath = s"macrobenchmarks/datasets/$graphDataset"
    println(s"Dataset Path = $datasetPath")

    val dataset = reader.readDataset(sc, datasetPath)
    algorithm.run(graph.generate(dataset))

    metricsCollector.stop()

    val logs = new PrintStream(s"macrobenchmarks/reports/metrics-$implementation-$graphAlgorithm-$graphDataset.txt")
    metricsCollector.printCollectedMetrics(logs)

    sc.stop()
  }
}
