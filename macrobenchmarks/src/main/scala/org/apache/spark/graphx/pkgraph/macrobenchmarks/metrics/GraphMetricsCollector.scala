package org.apache.spark.graphx.pkgraph.macrobenchmarks.metrics

import org.apache.spark.SparkContext
import org.apache.spark.metrics.ExecutorMetricType
import org.apache.spark.scheduler.{
  SparkListener,
  SparkListenerApplicationEnd,
  SparkListenerApplicationStart,
  SparkListenerExecutorMetricsUpdate,
  SparkListenerStageCompleted,
  SparkListenerStageExecutorMetrics,
  SparkListenerTaskEnd,
  SparkListenerTaskStart
}

import java.io.PrintStream
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class GraphMetricsCollector(private val sc: SparkContext, impl: String, algorithmName: String, datasetName: String) {
  private var startTime = System.currentTimeMillis()
  private var endTime = 0L
  private val executors = mutable.HashMap[String, GraphExecutorMetrics]()
  private val stages = ArrayBuffer.empty[GraphStageMetrics]

  private def totalTime: Long = endTime - startTime

  sc.addSparkListener(new SparkListener {
    override def onStageCompleted(stage: SparkListenerStageCompleted): Unit = {
      stages += GraphStageMetrics(
        stage.stageInfo.stageId,
        stage.stageInfo.name,
        stage.stageInfo.numTasks,
        stage.stageInfo.getStatusString,
        stage.stageInfo.attemptNumber(),
        stage.stageInfo.completionTime.getOrElse(0L) - stage.stageInfo.submissionTime.getOrElse(0L),
        stage.stageInfo.taskMetrics.executorCpuTime
      )
    }

    override def onExecutorMetricsUpdate(executor: SparkListenerExecutorMetricsUpdate): Unit = {
      for ((_, metrics) <- executor.executorUpdates) {
        /*
        val executorMetrics = GraphExecutorMetrics(
          metrics.getMetricValue("peakMemoryMetrics.JVMHeapMemory"),
          metrics.getMetricValue("peakMemoryMetrics.OnHeapExecutionMemory"),
          metrics.getMetricValue("peakMemoryMetrics.OnHeapStorageMemory")
        )
        executors(executor.execId).combine(executorMetrics)

         */
      }
    }
  })

  def start(): Unit = {
    startTime = System.currentTimeMillis()
  }

  def stop(): Unit = {
    endTime = System.currentTimeMillis()
  }

  def printCollectedMetrics(out: PrintStream = Console.out): Unit = {
    out.println(s"------------- [METRICS | $impl | $algorithmName | $datasetName] -------------")
    out.println(s"- Stage Count: ${stages.length}")
    out.println(s"- Total Time: $totalTime ms (start: $startTime, end: $endTime)")

    for ((id, executor) <- executors) {
      out.println(s"- Executor ($id)")
      out.println(s"\t+ JVM Heap Memory: ${executor.heapMemory}")
      out.println(s"\t+ Execution Memory: ${executor.executionMemory}")
      out.println(s"\t+ Storage Memory: ${executor.storageMemory}")
    }

    for (stage <- stages) {
      out.println(s"- Stage (${stage.id})")
      out.println(s"\t+ Name: ${stage.name}")
      out.println(s"\t+ Status: ${stage.status}")
      out.println(s"\t+ Attempt Number: ${stage.attemptNumber}")
      out.println(s"\t+ Total Time: ${stage.totalTime} ms")
      out.println(s"\t+ Executor CPU Time: ${stage.cpuTime} ns (${stage.cpuTime / 1000000} ms)")
    }

    out.println("------------- [METRICS | END] -------------")
  }
}
