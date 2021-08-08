package org.apache.spark.graphx.pkgraph.microbenchmarks

import org.apache.spark.graphx.TripletFields
import org.apache.spark.graphx.impl.EdgeActiveness
import org.apache.spark.graphx.pkgraph.microbenchmarks.builders.{GraphXPartitionBuilder, PKGraphPartitionBuilder}
import org.scalameter.api.Gen

object AggregateMessagesSrcIndexBenchmark extends Microbenchmark("GraphX") {
  private val fixedSize: Int = 1000000
  private lazy val activeVertices: Gen[Float] = Gen
    .range("% active vertices")(10, 100, 10)
    .map(p => p.toFloat / 100f)

  private lazy val graphXPartitions =
    activeVertices.map(percentage => GraphXPartitionBuilder.build(fixedSize, 1.0f, percentage))
  private lazy val k2Partitions =
    activeVertices.map(percentage => PKGraphPartitionBuilder.build(2, fixedSize, 1.0f, percentage))
  private lazy val k4Partitions =
    activeVertices.map(percentage => PKGraphPartitionBuilder.build(4, fixedSize, 1.0f, percentage))
  private lazy val k8Partitions =
    activeVertices.map(percentage => PKGraphPartitionBuilder.build(8, fixedSize, 1.0f, percentage))

  performance of "AggregateMessagesSrcIndexBenchmark" in {
    using(graphXPartitions) curve "GraphX" in { partition =>
      partition
        .aggregateMessagesIndexScan[Int](ctx => ctx.sendToSrc(10), _ + _, TripletFields.All, EdgeActiveness.SrcOnly)
    }

    using(k2Partitions) curve "PKGraph (k=2)" in { partition =>
      partition
        .aggregateMessages[Int](ctx => ctx.sendToSrc(10), _ + _, TripletFields.All, EdgeActiveness.SrcOnly)
    }

    using(k4Partitions) curve "PKGraph (k=4)" in { partition =>
      partition
        .aggregateMessages[Int](ctx => ctx.sendToSrc(10), _ + _, TripletFields.All, EdgeActiveness.SrcOnly)
    }

    using(k8Partitions) curve "PKGraph (k=8)" in { partition =>
      partition
        .aggregateMessages[Int](ctx => ctx.sendToSrc(10), _ + _, TripletFields.All, EdgeActiveness.SrcOnly)
    }
  }
}
