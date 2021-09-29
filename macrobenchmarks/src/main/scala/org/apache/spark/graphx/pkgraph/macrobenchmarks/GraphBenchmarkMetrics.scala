package org.apache.spark.graphx.pkgraph.macrobenchmarks

import org.json4s.JValue
import org.json4s.JsonDSL.WithDouble._
import org.json4s.jackson.JsonMethods._

import java.text.SimpleDateFormat
import java.time.Instant
import java.util.Date

case class GraphWorkloadMetrics(
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
    partitions: Int,
    useGridPartition: Boolean,
    memorySize: Long,
    workloads: Seq[GraphWorkloadMetrics]
) {
  def toJsonValue: JValue = {
    val allCpuUsageValues = workloads.flatMap(m => m.cpuUsageValues)
    val averageOverallCpuUsage = if(allCpuUsageValues.isEmpty) 0 else allCpuUsageValues.sum / allCpuUsageValues.size

    ("implementation" -> implementation) ~
      ("dataset" -> dataset) ~
      ("warmup" -> warmup) ~
      ("samples" -> samples) ~
      ("partitions" -> partitions) ~
      ("useGridPartition" -> useGridPartition) ~
      ("memorySize" -> memorySize) ~
      ("averageOverallCpuUsage" -> averageOverallCpuUsage) ~
      ("workloads" -> workloads.map { m =>
        ("workload" -> m.workload) ~
          ("latency" ->
            ("values" -> m.latencyValues) ~
              ("average" -> {if(m.latencyValues.isEmpty) 0 else m.latencyValues.sum / m.latencyValues.size})) ~
          ("throughput" ->
            ("values" -> m.throughputValues) ~
              ("average" -> {if(m.throughputValues.isEmpty) 0 else m.throughputValues.sum / m.throughputValues.size})) ~
          ("cpuUsage" ->
            ("values" -> m.cpuUsageValues) ~
              ("average" -> {if(m.cpuUsageValues.isEmpty) 0 else m.cpuUsageValues.sum / m.cpuUsageValues.size}))
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
