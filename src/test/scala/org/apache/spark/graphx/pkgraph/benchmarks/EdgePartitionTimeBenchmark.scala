package org.apache.spark.graphx.pkgraph.benchmarks

import org.apache.spark.graphx.impl.{EdgePartition, EdgePartitionBuilder}
import org.apache.spark.graphx.pkgraph.graph.{PKEdgePartition, PKEdgePartitionBuilder}
import org.scalameter.api._

object EdgePartitionTimeBenchmark extends Bench.OfflineReport {
  lazy val partitionSizes: Gen[Int] = Gen.range("partition_size")(10000, 50000, 10000)

  lazy val graphXPartitions: Gen[EdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    val builder = new EdgePartitionBuilder[Int, Int](size)

    // Add edges
    for (i <- 0 until math.floor(math.sqrt(size)).toInt) {
      for (j <- 0 until math.floor(math.sqrt(size)).toInt) {
        builder.add(i, j, j + i)
      }
    }

    builder.toEdgePartition
  }

  lazy val k2Partitions: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    buildPKEdgePartition(2, size)
  }

  lazy val k4Partitions: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    buildPKEdgePartition(4, size)
  }

  lazy val k8Partitions: Gen[PKEdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    buildPKEdgePartition(8, size)
  }

  performance of "Edge Partitions Operations" in {
    measure method "map" in {
      using(graphXPartitions) in {
        _.map(e => e.attr * 2)
      }

      using(k2Partitions) in {
        _.map(e => e.attr * 2)
      }

      using(k4Partitions) in {
        _.map(e => e.attr * 2)
      }

      using(k8Partitions) in {
        _.map(e => e.attr * 2)
      }
    }
  }

  private def buildPKEdgePartition(k: Int, size: Int): PKEdgePartition[Int, Int] = {
    val builder = PKEdgePartitionBuilder[Int, Int](k, size)

    // Add edges
    for (i <- 0 until math.floor(math.sqrt(size)).toInt) {
      for (j <- 0 until math.floor(math.sqrt(size)).toInt) {
        builder.add(i, j, j + i)
      }
    }

    builder.build
  }
}
