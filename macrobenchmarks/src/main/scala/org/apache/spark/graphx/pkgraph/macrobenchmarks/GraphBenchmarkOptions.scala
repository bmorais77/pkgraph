package org.apache.spark.graphx.pkgraph.macrobenchmarks

import org.apache.commons.cli.{Option, Options, PosixParser}

case class GraphBenchmarkOptions(
    implementationFilter: String,
    workloadFilter: String,
    datasetFilter: String,
    datasetDir: String,
    warmup: Int,
    samples: Int,
    memoryTests: Boolean,
    buildTests: Boolean,
    iterationTests: Boolean,
    throughputTests: Boolean,
    cpuUsageTests: Boolean,
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

    val memoryTest = new Option("MemoryTests", true, "Whether to perform a memory test as well")
    memoryTest.setRequired(false)
    options.addOption(memoryTest)

    val buildTests = new Option("BuildTests", true, "Whether to perform build tests")
    buildTests.setRequired(false)
    options.addOption(buildTests)

    val iterationTests = new Option("IterationTests", true, "Whether to perform iteration tests")
    iterationTests.setRequired(false)
    options.addOption(iterationTests)

    val throughputTests = new Option("ThroughputTests", true, "Whether to perform throughput tests")
    throughputTests.setRequired(false)
    options.addOption(throughputTests)

    val cpuUsageTests = new Option("CPUUsageTests", true, "Whether to perform cpu usage tests")
    cpuUsageTests.setRequired(false)
    options.addOption(cpuUsageTests)

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
      cmd.getOptionValue("MemoryTests", "false").toBoolean,
      cmd.getOptionValue("BuildTests", "false").toBoolean,
      cmd.getOptionValue("IterationTests", "false").toBoolean,
      cmd.getOptionValue("ThroughputTests", "false").toBoolean,
      cmd.getOptionValue("CPUUsageTests", "false").toBoolean,
      cmd.getOptionValue("Output")
    )
  }
}
