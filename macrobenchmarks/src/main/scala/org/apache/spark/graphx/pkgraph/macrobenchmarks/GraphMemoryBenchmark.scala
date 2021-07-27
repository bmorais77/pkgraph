package org.apache.spark.graphx.pkgraph.macrobenchmarks

import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.graphx.pkgraph.graph.PKGraph
import org.apache.spark.graphx.pkgraph.macrobenchmarks.datasets.MTXGraphDatasetReader
import org.apache.spark.graphx.pkgraph.macrobenchmarks.generators.{GraphGenerator, GraphXGenerator, PKGraphGenerator}
import org.apache.spark.util.SizeEstimator
import org.apache.spark.{SparkConf, SparkContext}

import java.io.PrintStream

object GraphMemoryBenchmark {
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

  def main(args: Array[String]): Unit = {
    assert(
      args.length >= 2,
      "Wrong usage: graph-memory-benchmark <implementation> <dataset> [<partition-count>]"
    )

    val implementation = args(0)
    val graphDataset = args(1)
    val partitionCount = if (args.length >= 5) args(4).toInt else -1

    val generator = getGraphGeneratorFromArgs(implementation)
    val reader = new MTXGraphDatasetReader

    val config = new SparkConf()
      .setMaster("local[4]")
      .setAppName(s"Graph Memory Benchmark ($implementation | $graphDataset)")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", "/tmp/spark-events")

    val sc = new SparkContext(config)

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

    val verticesEstimatedSize = SizeEstimator.estimate(graph.vertices.collect())
    val edgesEstimatedSize = if(implementation == "GraphX") {
      SizeEstimator.estimate(graph.edges.partitionsRDD.map(_._2).collect())
    } else {
      SizeEstimator.estimate(graph.asInstanceOf[PKGraph[Long, Int]].edges.edgePartitions.map(_._2).collect())
    }

    val report = new PrintStream(s"macrobenchmarks/reports/memory-metrics-$implementation-$graphDataset.txt")
    report.println(s"Implementation = $implementation")
    report.println(s"Dataset = $graphDataset")
    report.println(s"Partition Count = $partitionCount")
    report.println(s"Vertices Estimated Size = $verticesEstimatedSize bytes")
    report.println(s"Edges Estimated Size = $edgesEstimatedSize bytes")
    sc.stop()
  }
}
