package org.apache.spark.graphx.pkgraph.benchmarks

import org.apache.spark.graphx.impl.{EdgePartition, EdgePartitionBuilder}
import org.apache.spark.graphx.pkgraph.graph.{PKEdgePartition, PKEdgePartitionBuilder}
import org.scalameter.api._
import org.scalameter.Measurer

object EdgePartitionSpaceBenchmark extends Bench.OfflineReport {
  override def measurer: Measurer[Double] = new Measurer.MemoryFootprint

  lazy val partitionSizes: Gen[Int] = Gen.range("partition_size")(10000, 50000, 10000)

  lazy val graphXPartitions: Gen[EdgePartition[Int, Int]] = for { size <- partitionSizes } yield {
    buildGraphXEdgePartition(size)
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

  performance of "EdgePartition" in {
    performance of "build" in {
      using(graphXPartitions) in { partition =>
        partition
      }
    }
  }

  performance of "PKEdgePartition" in {
    performance of "build" in {
      using(k2Partitions) in { partition =>
        partition
      }

      using(k4Partitions) in { partition =>
        partition
      }

      using(k8Partitions) in { partition =>
        partition
      }
    }
  }

  private def buildGraphXEdgePartition(size: Int): EdgePartition[Int, Int] = {
    val builder = new EdgePartitionBuilder[Int, Int](size)

    // Add edges
    for (i <- 0 until math.floor(math.sqrt(size)).toInt) {
      for (j <- 0 until math.floor(math.sqrt(size)).toInt) {
        builder.add(i, j, j + i)
      }
    }

    builder.toEdgePartition
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
