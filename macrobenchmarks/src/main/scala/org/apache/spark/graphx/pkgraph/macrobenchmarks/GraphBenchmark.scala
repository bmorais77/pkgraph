package org.apache.spark.graphx.pkgraph.macrobenchmarks

import ch.cern.sparkmeasure.StageMetrics
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.Path
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.graphx.pkgraph.graph.PKGraph
import org.apache.spark.graphx.pkgraph.macrobenchmarks.workloads.{GraphWorkload, MapWorkload}
import org.apache.spark.graphx.pkgraph.macrobenchmarks.datasets.{GraphDataset, MTXGraphDatasetReader}
import org.apache.spark.graphx.pkgraph.macrobenchmarks.generators.GraphGenerator
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SizeEstimator
import org.json4s.jackson.prettyJson

import scala.collection.mutable.ArrayBuffer

object GraphBenchmark {
  private val implementations = Seq("GraphX", "PKGraph8")
  private val workloads = Seq("map", "pageRank", "triangleCount", "connectedComponents", "shortestPath")
  private val datasets = Seq("eu-2005", "indochina-2004", "soc-youtube-growth", "uk-2002")

  /**
    * Memory tests give metrics about the estimated size of the entire graph in memory.
    *
    * @param graph     Graph to test
    * @return metrics of the memory test
    */
  def runMemoryTest(graph: Graph[Long, Int]): GraphMemoryTestMetrics = {
    val verticesEstimatedSize =
      graph.vertices.partitionsRDD
        .aggregate(0L)((acc, part) => acc + SizeEstimator.estimate(part), (v1, v2) => v1 + v2)

    val edgesEstimatedSize = if (graph.isInstanceOf[GraphImpl[Long, Int]]) {
      graph.edges.partitionsRDD
        .aggregate(0L)((acc, part) => acc + SizeEstimator.estimate(part._2), (v1, v2) => v1 + v2)
    } else {
      graph
        .asInstanceOf[PKGraph[Long, Int]]
        .edges
        .edgePartitions
        .aggregate(0L)((acc, part) => acc + SizeEstimator.estimate(part._2), (v1, v2) => v1 + v2)
    }

    GraphMemoryTestMetrics(verticesEstimatedSize + edgesEstimatedSize)
  }

  /**
    * Build tests involve simply build a new graph from an existing dataset.
    * For each build test the latency of building the graph is taken.
    *
    * @param spark       Spark session
    * @param warmup      Number of warmup rounds
    * @param samples     Number of samples to take
    * @param generator   Graph generator to use
    * @param dataset     Dataset to create graph from
    * @return metrics of the build test
    */
  def runBuildTest(
      spark: SparkSession,
      warmup: Int,
      samples: Int,
      generator: GraphGenerator,
      dataset: GraphDataset
  ): GraphBuildTestMetrics = {
    println(s"### [TEST|BUILD|WARMUP=$warmup]")

    // Warmup
    for (_ <- 0 until warmup) {
      val graph = generator.generate(dataset)
      graph.edges.count() // Forces graph to be built
    }

    println(s"### [TEST|BUILD|SAMPLES|$samples]")

    // Take samples
    val testSamples = ArrayBuffer[Long]()
    for (_ <- 0 until samples) {
      val graph = generator.generate(dataset)
      val stageMetrics = StageMetrics(spark)
      stageMetrics.runAndMeasure { graph.edges.count() }

      val latency = stageMetrics.reportMap.get("elapsedTime").toLong
      testSamples += latency
    }

    println(s"### [TEST|BUILD|DONE]")
    GraphBuildTestMetrics(testSamples)
  }

  /**
    * Iteration tests takes samples of the latency values of executing various graph workloads.
    *
    * @param spark       Spark session
    * @param warmup      Number of warmup rounds
    * @param samples     Number of samples to take
    * @param graph       Graph to run workloads on
    * @param workload    Workload to execute on graph
    * @return metrics of the iteration test
    */
  def runIterationTest(
      spark: SparkSession,
      warmup: Int,
      samples: Int,
      graph: Graph[Long, Int],
      workloadName: String,
      workload: GraphWorkload
  ): GraphIterationTestMetrics = {
    println(s"### [TEST|ITERATION|WARMUP=$warmup]")

    // Warmup
    for (_ <- 0 until warmup) {
      workload.run(graph)
    }

    println(s"### [TEST|ITERATION|SAMPLES|$samples]")

    // Take samples
    val testSamples = ArrayBuffer[Long]()
    for (_ <- 0 until samples) {
      val stageMetrics = StageMetrics(spark)
      stageMetrics.runAndMeasure {
        workload.run(graph)
      }

      val latency = stageMetrics.reportMap.get("elapsedTime").toLong
      testSamples += latency
    }

    println(s"### [TEST|ITERATION|DONE]")
    GraphIterationTestMetrics(workloadName, testSamples)
  }

  /**
    * Throughput tests measure the throughput of a graph by executing a predefined workload.
    *
    * @param spark       Spark session
    * @param warmup      Number of warmup rounds
    * @param samples     Number of samples to take
    * @param graph       Graph to run workloads on
    * @return metrics of the throughput test
    */
  def runThroughputTest(
      spark: SparkSession,
      warmup: Int,
      samples: Int,
      graph: Graph[Long, Int]
  ): GraphThroughputTestMetrics = {
    val workload = new MapWorkload()

    println(s"### [TEST|THROUGHPUT|WARMUP=$warmup]")

    // Warmup
    for (_ <- 0 until warmup) {
      workload.run(graph)
    }

    println(s"### [TEST|THROUGHPUT|SAMPLES|$samples]")

    // Take samples
    val testSamples = ArrayBuffer[Long]()
    val recordTestSamples = ArrayBuffer[Long]()

    for (_ <- 0 until samples) {
      val stageMetrics = StageMetrics(spark)
      stageMetrics.runAndMeasure {
        workload.run(graph)
      }

      val metricsMap = stageMetrics.reportMap
      val latency = metricsMap.get("elapsedTime").toLong
      val throughput = (metricsMap.get("bytesRead").toLong / (latency.toFloat / 1000f)).toLong
      val recordThroughput = (metricsMap.get("recordsRead").toLong / (latency.toFloat / 1000f)).toLong

      testSamples += throughput
      recordTestSamples += recordThroughput
    }

    println(s"### [TEST|THROUGHPUT|DONE]")
    GraphThroughputTestMetrics(testSamples, recordTestSamples)
  }

  /**
    * CPU usages tests take the percentage of the total execution time spent on the CPU.
    *
    * @param spark       Spark session
    * @param warmup      Number of warmup rounds
    * @param samples     Number of samples to take
    * @param graph       Graph to run workloads on
    * @return metrics of the throughput test
    */
  def runCpuUsageTest(
      spark: SparkSession,
      warmup: Int,
      samples: Int,
      graph: Graph[Long, Int]
  ): GraphCpuUsageTestMetrics = {
    val workload = new MapWorkload()

    println(s"### [TEST|CPU_USAGE|WARMUP=$warmup]")

    // Warmup
    for (_ <- 0 until warmup) {
      workload.run(graph)
    }

    println(s"### [TEST|CPU_USAGE|SAMPLES|$samples]")

    // Take samples
    val testSamples = ArrayBuffer[Float]()
    for (_ <- 0 until samples) {
      val stageMetrics = StageMetrics(spark)
      stageMetrics.runAndMeasure {
        workload.run(graph)
      }

      val metricsMap = stageMetrics.reportMap
      val cpuUsage = metricsMap.get("executorCpuTime").toFloat / metricsMap.get("executorRunTime").toFloat
      testSamples += cpuUsage
    }

    println(s"### [TEST|CPU_USAGE|DONE]")
    GraphCpuUsageTestMetrics(testSamples)
  }

  def main(args: Array[String]): Unit = {
    val options = GraphBenchmarkParser.parse(args)

    val filteredImplementations = implementations.filter(i => options.implementationFilter.r.findFirstIn(i).isDefined)
    val filteredWorkloads = workloads.filter(i => options.workloadFilter.r.findFirstIn(i).isDefined)
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
    # - Memory Tests: ${options.memoryTests}
    # - Build Tests: ${options.buildTests}
    # - Iteration Tests: ${options.iterationTests}
    # - Throughput Tests: ${options.throughputTests}
    # - CPU Usage Tests: ${options.cpuUsageTests}
    #####################################################################################################
    \n\n""")

    val reader = new MTXGraphDatasetReader
    val sc = new SparkContext()
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()

    for (impl <- filteredImplementations) {
      val metrics = ArrayBuffer[GraphBenchmarkMetrics]()

      println(s"### [IMPL=$impl] ###")

      for (dataset <- filteredDatasets) {
        val generator = GraphGenerator.fromString(impl)
        val datasetPath = s"${options.datasetDir}/$dataset.mtx"
        val graphDataset = reader.readDataset(sc, datasetPath)
        val graph = generator.generate(graphDataset).cache()

        println(s"### [DATASET=$dataset] ###")

        var memoryTestMetrics: Option[GraphMemoryTestMetrics] = Option.empty
        if (options.memoryTests) {
          memoryTestMetrics = Option(runMemoryTest(graph))
        }

        var buildTestMetrics: Option[GraphBuildTestMetrics] = Option.empty
        if (options.buildTests) {
          buildTestMetrics = Option(runBuildTest(spark, options.warmup, options.samples, generator, graphDataset))
        }

        val iterationTestMetrics = ArrayBuffer[GraphIterationTestMetrics]()
        for (workload <- filteredWorkloads) {
          val graphWorkload = GraphWorkload.fromString(workload)

          if (options.iterationTests) {
            iterationTestMetrics += runIterationTest(
              spark,
              options.warmup,
              options.samples,
              graph,
              workload,
              graphWorkload
            )
          }
        }

        var throughputTestMetrics: Option[GraphThroughputTestMetrics] = Option.empty
        if (options.throughputTests) {
          throughputTestMetrics = Option(runThroughputTest(spark, options.warmup, options.samples, graph))
        }

        var cpuUsageTestMetrics: Option[GraphCpuUsageTestMetrics] = Option.empty
        if (options.cpuUsageTests) {
          cpuUsageTestMetrics = Option(runCpuUsageTest(spark, options.warmup, options.samples, graph))
        }

        // Graph no longer needed
        graph.unpersist()

        val benchmarkMetrics =
          GraphBenchmarkMetrics(
            impl,
            dataset,
            options.warmup,
            options.samples,
            memoryTestMetrics,
            buildTestMetrics,
            iterationTestMetrics,
            throughputTestMetrics,
            cpuUsageTestMetrics
          )

        println(prettyJson(benchmarkMetrics.toJsonValue))
        metrics += benchmarkMetrics
      }

      // Write the output of each implementation separately
      val now = System.currentTimeMillis()
      val output = s"${options.output}/graph-macrobenchmark-$impl-$now.json"

      println(s"/////////////////////////////////////////////////")
      println(s"// Finished testing for implementation '$impl'")
      println(s"// Writing to output: '$output'")
      println(s"/////////////////////////////////////////////////")

      val outputPath = new Path(output)
      val fs = outputPath.getFileSystem(sc.hadoopConfiguration)
      val out = fs.create(outputPath, true)
      out.writeBytes(GraphBenchmarkMetrics.toJsonArray(metrics))
      out.close()
    }

    sc.stop()
  }
}
