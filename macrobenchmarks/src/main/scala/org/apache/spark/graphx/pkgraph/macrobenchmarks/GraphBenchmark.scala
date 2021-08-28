package org.apache.spark.graphx.pkgraph.macrobenchmarks

import ch.cern.sparkmeasure.StageMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.commons.cli.{Option, Options, PosixParser}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.graphx.pkgraph.graph.PKGraph
import org.apache.spark.graphx.pkgraph.macrobenchmarks.workloads.GraphWorkload
import org.apache.spark.graphx.pkgraph.macrobenchmarks.datasets.{GraphDataset, MTXGraphDatasetReader}
import org.apache.spark.graphx.pkgraph.macrobenchmarks.generators.GraphGenerator
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SizeEstimator
import org.json4s.jackson.{Json, prettyJson}

import java.net.URI
import scala.collection.mutable.ArrayBuffer

object GraphBenchmark {
  case class GraphBenchmarkOptions(
      implementationFilter: String,
      workloadFilter: String,
      datasetFilter: String,
      datasetDir: String,
      warmup: Int,
      samples: Int,
      partitions: Int,
      performMemoryTest: Boolean,
      output: String
  )

  object GraphBenchmarkParser {
    private val options = new Options

    {
      val impl = new Option("IFilter", true, "Filter to apply to graph implementations")
      impl.setRequired(false)
      options.addOption(impl)

      val algorithm = new Option("WFilter", true, "Filter to apply to graph workloads")
      algorithm.setRequired(false)
      options.addOption(algorithm)

      val dataset = new Option("DFilter", true, "Filter to apply to datasets")
      dataset.setRequired(false)
      options.addOption(dataset)

      val datasetDir = new Option("DatasetDir", true, "Path to the directory containing the datasets")
      datasetDir.setRequired(true)
      options.addOption(datasetDir)

      val warmup = new Option("Warmup", true, "Number of warmup rounds for each test")
      warmup.setRequired(true)
      options.addOption(warmup)

      val samples = new Option("Samples", true, "Number of samples to take for each test")
      samples.setRequired(true)
      options.addOption(samples)

      val partitions =
        new Option("Partitions", true, "Number of partitions the graph should have (-1 means to not repartition graph)")
      partitions.setRequired(false)
      options.addOption(partitions)

      val memoryTest = new Option("PerformMemoryTest", true, "Whether to perform a memory test as well")
      memoryTest.setRequired(false)
      options.addOption(memoryTest)

      val output = new Option("Output", true, "Path to the output file to write report to")
      output.setRequired(true)
      options.addOption(output)
    }

    def parse(args: Array[String]): GraphBenchmarkOptions = {
      val cmdParse = new PosixParser
      val cmd = cmdParse.parse(options, args)

      GraphBenchmarkOptions(
        cmd.getOptionValue("IFilter", ".*"),
        cmd.getOptionValue("WFilter", ".*"),
        cmd.getOptionValue("DFilter", ".*"),
        cmd.getOptionValue("DatasetDir"),
        cmd.getOptionValue("Warmup").toInt,
        cmd.getOptionValue("Samples").toInt,
        cmd.getOptionValue("Partitions", "-1").toInt,
        cmd.getOptionValue("PerformMemoryTest", "false").toBoolean,
        cmd.getOptionValue("Output")
      )
    }
  }

  private val implementations = Seq("GraphX", "PKGraph8")
  private val algorithms = Seq("build", "map", "pageRank", "triangleCount", "connectedComponents", "shortestPath")
  private val datasets = Seq("eu-2005", "indochina-2004", "soc-youtube-growth", "uk-2002")

  def runBenchmark(
      generator: GraphGenerator,
      partitionCount: Int,
      algorithm: GraphWorkload,
      dataset: GraphDataset
  ): Unit = {
    val graph = generator.generate(dataset, partitionCount)
    algorithm.run(graph)
  }

  def runMemoryBenchmark(impl: String, generator: GraphGenerator, partitionCount: Int, dataset: GraphDataset): Long = {
    val graph = generator.generate(dataset, partitionCount)
    val verticesEstimatedSize =
      graph.vertices.partitionsRDD
        .aggregate(0L)((acc, part) => acc + SizeEstimator.estimate(part), (v1, v2) => v1 + v2)

    val edgesEstimatedSize = if (impl == "GraphX") {
      graph.edges.partitionsRDD
        .aggregate(0L)((acc, part) => acc + SizeEstimator.estimate(part._2), (v1, v2) => v1 + v2)
    } else {
      graph
        .asInstanceOf[PKGraph[Long, Int]]
        .edges
        .edgePartitions
        .aggregate(0L)((acc, part) => acc + SizeEstimator.estimate(part._2), (v1, v2) => v1 + v2)
    }

    verticesEstimatedSize + edgesEstimatedSize
  }

  def main(args: Array[String]): Unit = {
    val options = GraphBenchmarkParser.parse(args)

    val filteredImplementations = implementations.filter(i => options.implementationFilter.r.findFirstIn(i).isDefined)
    val filteredWorkloads = algorithms.filter(i => options.workloadFilter.r.findFirstIn(i).isDefined)
    val filteredDatasets = datasets.filter(i => options.datasetFilter.r.findFirstIn(i).isDefined)

    println(s"""
    #####################################################################################################
    # Macrobenchmarks
    # - IFilter: ${options.implementationFilter}
    # - WFilter: ${options.workloadFilter}
    # - DFilter: ${options.datasetFilter}
    # - Dataset Directory: ${options.datasetDir}
    # 
    # - Implementations: ${filteredImplementations.mkString("[", ",", "]")}
    # - Workloads: ${filteredWorkloads.mkString("[", ",", "]")}
    # - Datasets: ${filteredDatasets.mkString("[", ",", "]")}
    #
    # - Warmup: ${options.warmup}
    # - Samples: ${options.samples}
    # - Partitions: ${options.partitions}
    # - Perform Memory Test: ${options.performMemoryTest}
    #####################################################################################################
    """)

    val reader = new MTXGraphDatasetReader
    val sc = new SparkContext()
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()

    val metrics = ArrayBuffer[GraphBenchmarkMetrics]()
    for (impl <- filteredImplementations) {
      for (dataset <- filteredDatasets) {
        val generator = GraphGenerator.fromString(impl)
        val datasetPath = s"${options.datasetDir}/$dataset.mtx"
        val graphDataset = reader.readDataset(sc, datasetPath)
        val algorithmMetrics = ArrayBuffer[GraphWorkloadMetrics]()

        for (workload <- filteredWorkloads) {
          val graphWorkload = GraphWorkload.fromString(workload)

          println(s"""
          #####################################################################################################
          # Macrobenchmark
          # - Implementation: $impl
          # - Dataset: $datasetPath
          # - Workload: $workload
          #####################################################################################################
          """)

          val warmupStart = System.currentTimeMillis()
          println(s"Warming up (${options.warmup})...")
          for (_ <- 0 until options.warmup) {
            runBenchmark(generator, options.partitions, graphWorkload, graphDataset)
          }
          val warmupEnd = System.currentTimeMillis()
          println(s"Warmup done (${warmupEnd - warmupStart}ms)")

          val latencyValues = ArrayBuffer[Long]()
          val throughputValues = ArrayBuffer[Long]()
          val cpuUsageValues = ArrayBuffer[Float]()

          println(s"Running ${options.samples} sample(s)...")
          for (i <- 0 until options.samples) {
            val stageMetrics = StageMetrics(spark)
            println(s"////// Sample #${i + 1} //////")
            stageMetrics.runAndMeasure {
              runBenchmark(generator, options.partitions, graphWorkload, graphDataset)
            }
            println(s"//////////////////////////////")

            val metricsMap = stageMetrics.reportMap
            val latency = metricsMap.get("elapsedTime").toLong
            val throughput = (metricsMap.get("recordsRead").toLong / (latency.toFloat / 1000f)).toLong
            val cpuUsage = metricsMap.get("executorCpuTime").toFloat / metricsMap.get("executorRunTime").toFloat

            latencyValues += latency
            throughputValues += throughput
            cpuUsageValues += cpuUsage
          }

          algorithmMetrics += GraphWorkloadMetrics(workload, latencyValues, throughputValues, cpuUsageValues)
        }

        var memory = 0L
        if (options.performMemoryTest) {
          val memoryTestStart = System.currentTimeMillis()
          println("Running memory test...")
          memory = runMemoryBenchmark(impl, generator, options.partitions, graphDataset)
          val memoryTestEnd = System.currentTimeMillis()
          println(s"Memory test done (${memoryTestEnd - memoryTestStart}ms)")
        }

        val benchmarkMetrics =
          GraphBenchmarkMetrics(impl, dataset, options.warmup, options.samples, memory, algorithmMetrics)

        println(prettyJson(benchmarkMetrics.toJsonValue))
        metrics += benchmarkMetrics
      }
    }

    val now = System.currentTimeMillis()
    val output = s"${options.output}/graph-macrobenchmark-$now.json"
    println(s"Output = $output")

    val outputPath = new Path(output)
    val fs = outputPath.getFileSystem(sc.hadoopConfiguration)
    val out = fs.create(outputPath, true)
    out.writeBytes(GraphBenchmarkMetrics.toJsonArray(metrics))
    out.close()
    sc.stop()
  }
}
