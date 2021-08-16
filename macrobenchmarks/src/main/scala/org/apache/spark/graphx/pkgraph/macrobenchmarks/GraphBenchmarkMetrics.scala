package org.apache.spark.graphx.pkgraph.macrobenchmarks

import org.json4s.JValue
import org.json4s.JsonDSL.WithDouble._
import org.json4s.jackson.JsonMethods._

import java.text.SimpleDateFormat
import java.time.Instant
import java.util.Date

case class GraphAlgorithmMetrics(
    workload: String,
    latencyValues: Seq[Long],
    throughputValues: Seq[Long],
    cpuUsageValues: Seq[Float]
)

case class GraphBenchmarkMetrics(
    implementation: String,
    dataset: String,
    warmup: Int,
    samples: Int,
    memorySize: Long,
    algorithmMetrics: Seq[GraphAlgorithmMetrics]
) {
  def toJsonValue: JValue = {
    val allCpuUsageValues = algorithmMetrics.flatMap(m => m.cpuUsageValues)
    val averageOverallCpuUsage = allCpuUsageValues.sum / allCpuUsageValues.size

    ("implementation" -> implementation) ~
      ("dataset" -> dataset) ~
      ("warmup" -> warmup) ~
      ("samples" -> samples) ~
      ("memorySize" -> memorySize) ~
      ("averageOverallCpuUsage" -> averageOverallCpuUsage) ~
      ("algorithms" -> algorithmMetrics.map { m =>
        ("workload" -> m.workload) ~
          ("latency" ->
            ("values" -> m.latencyValues) ~
              ("average" -> m.latencyValues.sum / m.latencyValues.size)) ~
          ("throughput" ->
            ("values" -> m.throughputValues) ~
              ("average" -> m.throughputValues.sum / m.throughputValues.size)) ~
          ("cpuUsage" ->
            ("values" -> m.cpuUsageValues) ~
              ("average" -> m.cpuUsageValues.sum / m.cpuUsageValues.size))
      })
  }
}

object GraphBenchmarkMetrics {
  def toJsonArray(metrics: Seq[GraphBenchmarkMetrics]): String = {
    val now = new SimpleDateFormat("HH:mm:ss dd/MM/YYYY").format(Date.from(Instant.now()))
    val json = ("timestamp" -> now) ~ ("benchmarks" -> metrics.map(_.toJsonValue))
    pretty(render(json))
  }
}
