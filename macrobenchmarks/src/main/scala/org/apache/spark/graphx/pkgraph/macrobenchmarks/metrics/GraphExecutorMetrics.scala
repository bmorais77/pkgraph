package org.apache.spark.graphx.pkgraph.macrobenchmarks.metrics

case class GraphExecutorMetrics(
    var heapMemory: Long = 0L,
    var executionMemory: Long = 0L,
    var storageMemory: Long = 0L
) {
  def combine(other: GraphExecutorMetrics): GraphExecutorMetrics = {
    GraphExecutorMetrics(
      math.max(heapMemory, other.heapMemory),
      math.max(executionMemory, other.executionMemory),
      math.max(storageMemory, other.storageMemory)
    )
  }
}
