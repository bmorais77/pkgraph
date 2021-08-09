package org.apache.spark.graphx.pkgraph.macrobenchmarks

import ch.cern.sparkmeasure.StageMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.commons.cli.{Option, Options, ParseException, PosixParser}
import org.apache.spark.graphx.pkgraph.macrobenchmarks.algorithms.GraphAlgorithm
import org.apache.spark.graphx.pkgraph.macrobenchmarks.datasets.MTXGraphDatasetReader
import org.apache.spark.graphx.pkgraph.macrobenchmarks.generators.GraphGenerator
import org.apache.spark.sql.SparkSession

import java.io.{File, PrintStream}

object GraphBenchmark {
  case class GraphBenchmarkOptions(implementation: String, algorithm: String, input: String, output: String)

  object GraphBenchmarkParser {
    private val options = new Options

    {
      val implementationOption =
        new Option("implementation", true, "Graph implementation to use (i.e 'PKGraph<N>' or 'GraphX')")
      implementationOption.setRequired(true)
      options.addOption(implementationOption)

      val algorithmOption = new Option("algorithm", true, "Graph algorithm to execute")
      algorithmOption.setRequired(true)
      options.addOption(algorithmOption)

      val inputOption = new Option("input", true, "Path to the input file containing the graph data")
      inputOption.setRequired(true)
      options.addOption(inputOption)

      val outputOption = new Option("output", true, "Path to the local output file to write report to")
      outputOption.setRequired(true)
      options.addOption(outputOption)
    }

    def parse(args: Array[String]): GraphBenchmarkOptions = {
      val cmdParse = new PosixParser
      val cmd = cmdParse.parse(options, args)

      if (cmd.getOptionValue("input") == null) {
        throw new ParseException("no input file specified")
      }

      GraphBenchmarkOptions(
        cmd.getOptionValue("implementation"),
        cmd.getOptionValue("algorithm"),
        cmd.getOptionValue("input"),
        cmd.getOptionValue("output")
      )
    }
  }

  def main(args: Array[String]): Unit = {
    val options = GraphBenchmarkParser.parse(args)

    val generator = GraphGenerator.fromString(options.implementation)
    val algorithm = GraphAlgorithm.fromString(options.algorithm)
    val reader = new MTXGraphDatasetReader

    val config = new SparkConf()
    //.setMaster("local[4]")
      .setAppName(s"Graph Benchmark (${options.implementation} | ${options.algorithm} | ${options.input})")
    //.set("spark.eventLog.enabled", "true")
    //.set("spark.eventLog.dir", "/tmp/spark-events")

    val sc = new SparkContext(config)
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()

    val dataset = reader.readDataset(sc, options.input)
    val graph = generator.generate(dataset)
    val stageMetrics = StageMetrics(spark)
    stageMetrics.runAndMeasure {
      algorithm.run(graph.persist())
    }

    val now = System.currentTimeMillis()
    val outputPath = s"${options.output}-$now.txt"
    println(outputPath)

    val file = new File(outputPath)
    file.createNewFile()

    val report = new PrintStream(file)
    report.println(s"Implementation = ${options.implementation}")
    report.println(s"Algorithm = ${options.algorithm}")
    report.println(s"Input = ${options.input}")
    report.println(stageMetrics.report())
    sc.stop()
  }
}
