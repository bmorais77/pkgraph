package org.apache.spark.graphx.pkgraph.microbenchmarks

import org.apache.spark.graphx.TripletFields
import org.apache.spark.graphx.impl.{EdgeActiveness, EdgePartition}
import org.apache.spark.graphx.pkgraph.graph.PKEdgePartition
import org.apache.spark.graphx.pkgraph.microbenchmarks.datasets.{GraphXDataSet, PKGraphDataSet}
import org.scalameter.api.{Bench, Gen}

object EdgePartitionAggregateBenchmark extends Bench.OfflineReport {
  private val partitionSize = 10000
  private val partitionSparsity = 0.1f

  lazy val activeFactors: Gen[Int] = Gen.range("active percentage")(10, 100, 10)

  def buildGraphXAggregateMessagesPartitions(): Gen[EdgePartition[Int, Int]] = {
    for { factor <- activeFactors } yield {
      val partition = GraphXDataSet.buildGraphXEdgePartition(partitionSize, partitionSparsity)
      val sqrSize = math.floor(math.sqrt(partitionSize)).toInt

      val activeFactor = factor.toFloat / 100f
      println(activeFactor)
      val vertices = (0 until (sqrSize * activeFactor).toInt).map(i => i.toLong)
      partition.withActiveSet(vertices.iterator)
    }
  }

  def buildPKAggregateMessagesPartitions(k: Int): Gen[PKEdgePartition[Int, Int]] = {
    for { factor <- activeFactors } yield {
      val partition = PKGraphDataSet.buildPKGraphEdgePartition(k, partitionSize, partitionSparsity)
      val sqrSize = math.floor(math.sqrt(partitionSize)).toInt

      val activeFactor = factor.toFloat / 100f
      val vertices = (0 until (sqrSize * activeFactor).toInt).map(i => i.toLong)
      partition.withActiveSet(vertices.iterator)
    }
  }

  measure method "aggregateMessagesEdgeScan" in {
    using(buildGraphXAggregateMessagesPartitions()) curve "GraphX" in { partition =>
      partition
        .aggregateMessagesEdgeScan[Int](ctx => ctx.sendToSrc(10), _ + _, TripletFields.All, EdgeActiveness.Neither)
    }

    using(buildPKAggregateMessagesPartitions(2)) curve "PKGraph (k=2)" in { partition =>
      partition
        .aggregateMessagesEdgeScan[Int](ctx => ctx.sendToSrc(10), _ + _, TripletFields.All, EdgeActiveness.Neither)
    }

    using(buildPKAggregateMessagesPartitions(4)) curve "PKGraph (k=4)" in { partition =>
      partition
        .aggregateMessagesEdgeScan[Int](ctx => ctx.sendToSrc(10), _ + _, TripletFields.All, EdgeActiveness.Neither)
    }

    using(buildPKAggregateMessagesPartitions(8)) curve "PKGraph (k=8)" in { partition =>
      partition
        .aggregateMessagesEdgeScan[Int](ctx => ctx.sendToSrc(10), _ + _, TripletFields.All, EdgeActiveness.Neither)
    }
  }

  measure method "aggregateMessagesSrcIndexScan" in {
    using(buildGraphXAggregateMessagesPartitions()) curve "GraphX" in { partition =>
      partition
        .aggregateMessagesIndexScan[Int](ctx => ctx.sendToSrc(10), _ + _, TripletFields.All, EdgeActiveness.SrcOnly)
    }

    using(buildPKAggregateMessagesPartitions(2)) curve "PKGraph (k=2)" in { partition =>
      partition.aggregateMessagesSrcIndexScan[Int](
        ctx => ctx.sendToSrc(10),
        _ + _,
        TripletFields.All,
        EdgeActiveness.SrcOnly
      )
    }

    using(buildPKAggregateMessagesPartitions(4)) curve "PKGraph (k=4)" in { partition =>
      partition.aggregateMessagesSrcIndexScan[Int](
        ctx => ctx.sendToSrc(10),
        _ + _,
        TripletFields.All,
        EdgeActiveness.SrcOnly
      )
    }

    using(buildPKAggregateMessagesPartitions(8)) curve "PKGraph (k=8)" in { partition =>
      partition.aggregateMessagesSrcIndexScan[Int](
        ctx => ctx.sendToSrc(10),
        _ + _,
        TripletFields.All,
        EdgeActiveness.SrcOnly
      )
    }
  }

  measure method "aggregateMessagesDstIndexScan" in {
    using(buildGraphXAggregateMessagesPartitions()) curve "GraphX (Edge Scan)" in { partition =>
      partition
        .aggregateMessagesEdgeScan[Int](ctx => ctx.sendToDst(10), _ + _, TripletFields.All, EdgeActiveness.DstOnly)
    }

    using(buildPKAggregateMessagesPartitions(2)) curve "PKGraph (k=2)" in { partition =>
      partition.aggregateMessagesDstIndexScan[Int](
        ctx => ctx.sendToDst(10),
        _ + _,
        TripletFields.All,
        EdgeActiveness.DstOnly
      )
    }

    using(buildPKAggregateMessagesPartitions(4)) curve "PKGraph (k=4)" in { partition =>
      partition.aggregateMessagesDstIndexScan[Int](
        ctx => ctx.sendToDst(10),
        _ + _,
        TripletFields.All,
        EdgeActiveness.DstOnly
      )
    }

    using(buildPKAggregateMessagesPartitions(8)) curve "PKGraph (k=8)" in { partition =>
      partition.aggregateMessagesDstIndexScan[Int](
        ctx => ctx.sendToDst(10),
        _ + _,
        TripletFields.All,
        EdgeActiveness.DstOnly
      )
    }
  }
}
