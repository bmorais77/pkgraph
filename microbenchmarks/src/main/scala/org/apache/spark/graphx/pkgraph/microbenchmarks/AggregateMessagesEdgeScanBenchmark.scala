package org.apache.spark.graphx.pkgraph.microbenchmarks

import org.apache.spark.graphx.TripletFields
import org.apache.spark.graphx.impl.EdgeActiveness
import org.apache.spark.graphx.pkgraph.microbenchmarks.builders.{GraphXPartitionBuilder, PKGraphPartitionBuilder}
import org.scalameter.api.Gen

object AggregateMessagesEdgeScanBenchmark extends Microbenchmark("GraphX") {
  private lazy val edges: Gen[Int] = Gen.range("edges")(100000, 1000000, 100000)

  private lazy val graphXPartitions = edges.map(size => GraphXPartitionBuilder.build(size, 1.0f, 1.0f))
  private lazy val k2Partitions = edges.map(size => PKGraphPartitionBuilder.build(2, size, 1.0f, 1.0f))
  private lazy val k4Partitions = edges.map(size => PKGraphPartitionBuilder.build(4, size, 1.0f, 1.0f))
  private lazy val k8Partitions = edges.map(size => PKGraphPartitionBuilder.build(8, size, 1.0f, 1.0f))

  performance of "AggregateMessagesEdgeScanBenchmark" in {
    using(graphXPartitions) curve "GraphX" in { partition =>
      partition
        .aggregateMessagesEdgeScan[Int](ctx => ctx.sendToSrc(10), _ + _, TripletFields.All, EdgeActiveness.SrcOnly)
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
