package org.apache.spark.graphx.pkgraph.macrobenchmarks.metrics

case class GraphStageMetrics(
    id: Long,
    name: String,
    taskCount: Long,
    status: String,
    attemptNumber: Int,
    totalTime: Long,
    cpuTime: Long
)
