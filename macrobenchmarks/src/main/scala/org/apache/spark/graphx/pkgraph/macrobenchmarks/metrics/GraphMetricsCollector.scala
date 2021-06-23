package org.apache.spark.graphx.pkgraph.macrobenchmarks.metrics

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{
  SparkListener,
  SparkListenerApplicationEnd,
  SparkListenerApplicationStart,
  SparkListenerStageCompleted,
  SparkListenerTaskEnd
}

import java.io.PrintStream
import scala.collection.mutable.ArrayBuffer

class GraphMetricsCollector(private val sc: SparkContext, impl: String, algorithmName: String, datasetName: String) {
  private var startTime = System.currentTimeMillis()
  private var endTime = 0L
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
        stage.stageInfo.taskMetrics.executorCpuTime,
        stage.stageInfo.taskMetrics.peakExecutionMemory
      )
    }

    override def onApplicationStart(app: SparkListenerApplicationStart): Unit = {
      startTime = app.time
    }

    override def onApplicationEnd(app: SparkListenerApplicationEnd): Unit = {
      endTime = app.time
    }
  })

  def printCollectedMetrics(out: PrintStream = Console.out): Unit = {
    out.println(s"------------- [METRICS | $impl | $algorithmName | $datasetName] -------------")
    out.println(s"- Stage Count: ${stages.length}")
    out.println(s"- Total Time: $totalTime ms (start: $startTime, end: $endTime)")

    for (stage <- stages) {
      out.println(s"- Stage (${stage.id})")
      out.println(s"\t+ Name: ${stage.name}")
      out.println(s"\t+ Status: ${stage.status}")
      out.println(s"\t+ Attempt Number: ${stage.attemptNumber}")
      out.println(s"\t+ Total Time: ${stage.totalTime} ms")
      out.println(s"\t+ Executor CPU Time: ${stage.cpuTime} ns (${stage.cpuTime / 1000000} ms)")
      out.println(s"\t+ Peak Execution Memory: ${stage.peakExecutionMemory} bytes")
    }

    out.println("------------- [METRICS | END] -------------")
  }
}
