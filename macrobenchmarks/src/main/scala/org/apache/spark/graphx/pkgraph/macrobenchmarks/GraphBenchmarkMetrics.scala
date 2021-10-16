package org.apache.spark.graphx.pkgraph.macrobenchmarks

import org.json4s.JValue
import org.json4s.JsonDSL.WithDouble._
import org.json4s.jackson.JsonMethods._

import java.text.SimpleDateFormat
import java.time.Instant
import java.util.Date

case class GraphMemoryTestMetrics(estimatedSize: Long) {
  def toJsonValue: JValue = {
    ("estimatedSize" ->
      ("bytes" -> estimatedSize) ~
        ("kilobytes" -> estimatedSize / 1024) ~
        ("megabytes" -> estimatedSize / (1024 * 1024)) ~
        ("gigabytes" -> estimatedSize / (1024 * 1024 * 1024)))
  }
}

case class GraphBuildTestMetrics(latencyValues: Seq[Long]) {
  def toJsonValue: JValue = {
    ("latency" ->
      ("values" -> latencyValues) ~
        ("average" -> { if (latencyValues.isEmpty) 0 else latencyValues.sum / latencyValues.size }))
  }
}

case class GraphIterationTestMetrics(workload: String, latencyValues: Seq[Long]) {
  def toJsonValue: JValue = {
    ("workload" -> workload) ~
      ("latency" ->
        ("values" -> latencyValues) ~
          ("average" -> { if (latencyValues.isEmpty) 0 else latencyValues.sum / latencyValues.size }))
  }
}

case class GraphThroughputTestMetrics(throughputValues: Seq[Long], recordThroughputValues: Seq[Long]) {
  def toJsonValue: JValue = {
    ("throughput" ->
      ("values" -> throughputValues) ~
        ("average" -> { if (throughputValues.isEmpty) 0 else throughputValues.sum / throughputValues.size })) ~
      ("recordThroughput" ->
        ("values" -> recordThroughputValues) ~
          ("average" -> {
            if (recordThroughputValues.isEmpty) 0 else recordThroughputValues.sum / recordThroughputValues.size
          }))
  }
}

case class GraphCpuUsageTestMetrics(usageValues: Seq[Float]) {
  def toJsonValue: JValue = {
    ("cpuUsage" ->
      ("values" -> usageValues) ~
        ("average" -> { if (usageValues.isEmpty) 0 else usageValues.sum / usageValues.size }))
  }
}

case class GraphBenchmarkMetrics(
    implementation: String,
    dataset: String,
    warmup: Int,
    samples: Int,
    memoryTest: Option[GraphMemoryTestMetrics],
    buildTest: Option[GraphBuildTestMetrics],
    iterationTests: Seq[GraphIterationTestMetrics],
    throughputTests: Option[GraphThroughputTestMetrics],
    cpuUsageTests: Option[GraphCpuUsageTestMetrics]
) {
  def toJsonValue: JValue = {
    ("implementation" -> implementation) ~
      ("dataset" -> dataset) ~
      ("warmup" -> warmup) ~
      ("samples" -> samples) ~
      ("memoryTest" -> memoryTest.map(_.toJsonValue)) ~
      ("buildTest" -> buildTest.map(_.toJsonValue)) ~
      ("iterationTests" -> iterationTests.map(_.toJsonValue)) ~
      ("throughputTests" -> throughputTests.map(_.toJsonValue)) ~
      ("cpuUsageTests" -> cpuUsageTests.map(_.toJsonValue))
  }
}

object GraphBenchmarkMetrics {
  def toJsonArray(metrics: Seq[GraphBenchmarkMetrics]): String = {
    val now = new SimpleDateFormat("HH:mm:ss dd/MM/YYYY").format(Date.from(Instant.now()))
    val json = ("timestamp" -> now) ~ ("benchmarks" -> metrics.map(_.toJsonValue))
    pretty(render(json))
  }
}
