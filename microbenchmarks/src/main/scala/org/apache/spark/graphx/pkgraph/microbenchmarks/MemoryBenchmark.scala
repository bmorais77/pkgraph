package org.apache.spark.graphx.pkgraph.microbenchmarks

import org.apache.spark.graphx.pkgraph.microbenchmarks.builders.{GraphXPartitionBuilder, PKGraphPartitionBuilder}
import org.scalameter.api._
import org.scalameter.Measurer

object MemoryBenchmark extends Microbenchmark("GraphX") {
  override def measurer: Measurer[Double] = new Measurer.MemoryFootprint

  private val density = 1.0f
  private lazy val edges: Gen[Int] = Gen.range("edges")(100000, 1000000, 100000)
  private lazy val graphXPartitions = edges.map(size => GraphXPartitionBuilder.build(size, density, 0.0f))
  private lazy val k2Partitions = edges.map(size => PKGraphPartitionBuilder.build(2, size, density, 0.0f))
  private lazy val k4Partitions = edges.map(size => PKGraphPartitionBuilder.build(4, size, density, 0.0f))
  private lazy val k8Partitions = edges.map(size => PKGraphPartitionBuilder.build(8, size, density, 0.0f))

  performance of "MemoryBenchmark" in {
    using(graphXPartitions) curve "GraphX" in { partition =>
      partition
    }

    using(k2Partitions) curve "PKGraph (k=2)" in { partition =>
      partition
    }

    using(k4Partitions) curve "PKGraph (k=4)" in { partition =>
      partition
    }

    using(k8Partitions) curve "PKGraph (k=8)" in { partition =>
      partition
    }
  }
}
