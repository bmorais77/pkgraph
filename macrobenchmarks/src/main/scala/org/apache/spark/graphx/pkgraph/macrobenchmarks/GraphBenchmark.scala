package org.apache.spark.graphx.pkgraph.macrobenchmarks

import ch.cern.sparkmeasure.StageMetrics
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.graphx.pkgraph.graph.PKGraph
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.pkgraph.macrobenchmarks.algorithms.{ConnectedComponentsAlgorithm, GraphAlgorithm, PageRankAlgorithm, ShortestPathAlgorithm, TriangleCountAlgorithm}
import org.apache.spark.graphx.pkgraph.macrobenchmarks.datasets.GraphDatasetReader
import org.apache.spark.graphx.pkgraph.macrobenchmarks.datasets.readers.{EgoTwitterGraphDatasetReader, GeneratedGraphDatasetReader}
import org.apache.spark.graphx.pkgraph.macrobenchmarks.generators.{GraphGenerator, GraphXGenerator, PKGraphGenerator}
import org.apache.spark.sql.SparkSession

import java.io.PrintStream

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

  def getGraphDatasetReaderFromArgs(dataset: String): GraphDatasetReader = {
    dataset match {
      case "ego-twitter" => new EgoTwitterGraphDatasetReader
      case _             => new GeneratedGraphDatasetReader
    }
  }

  def main(args: Array[String]): Unit = {
    assert(args.length >= 3, "Wrong usage: graph-benchmark <implementation> <algorithm> <dataset> [<partition-count>]")

    val implementation = args(0)
    val graphAlgorithm = args(1)
    val graphDataset = args(2)
    val partitionCount = if (args.length >= 4) args(3).toInt else -1

    val generator = getGraphGeneratorFromArgs(implementation)
    val algorithm = getGraphAlgorithmFromArgs(graphAlgorithm)
    val reader = getGraphDatasetReaderFromArgs(graphDataset)

    val config = new SparkConf()
      .setMaster("local[4]")
      .setAppName(s"Graph Benchmark ($implementation | $graphAlgorithm | $graphDataset)")
      .set("spark.sql.unsafe.enabled", "true")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", "/tmp/spark-events")

    val sc = new SparkContext(config)
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()

    val datasetPath = s"datasets/$graphDataset"
    println(s"Dataset Path = $datasetPath")

    val dataset = reader.readDataset(sc, datasetPath)
    var graph = generator.generate(dataset)
    if (partitionCount != -1) {
      graph = graph match {
        case g: PKGraph[Long, Int] =>
          g.partitionByGridStrategy(partitionCount)
        case _ =>
          graph.partitionBy(PartitionStrategy.EdgePartition2D, partitionCount)
      }
    }

    // Warmup
    algorithm.run(graph)

    val stageMetrics = StageMetrics(spark)
    stageMetrics.runAndMeasure {
      algorithm.run(graph)
    }

    val report = new PrintStream(s"macrobenchmarks/reports/metrics-$implementation-$graphAlgorithm-$graphDataset.txt")
    report.println(s"Implementation = $implementation")
    report.println(s"Algorithm = $graphAlgorithm")
    report.println(s"Dataset = $graphDataset")
    report.println(s"Partition Count = $partitionCount")
    report.println(stageMetrics.report())
    sc.stop()
  }
}
