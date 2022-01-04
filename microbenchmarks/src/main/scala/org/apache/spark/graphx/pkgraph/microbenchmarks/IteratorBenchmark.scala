package org.apache.spark.graphx.pkgraph.microbenchmarks

import org.apache.spark.graphx.pkgraph.microbenchmarks.builders.{GraphXPartitionBuilder, PKGraphPartitionBuilder}
import org.scalameter.api.Gen

object IteratorBenchmark extends Microbenchmark("PKGraph (k=2)") {
  private lazy val edges: Gen[Int] = Gen.range("edges")(100000, 1000000, 100000)

  private lazy val graphXPartitions = edges.map(size => GraphXPartitionBuilder.build(size, 1.0f, 0.0f))
  private lazy val k2Partitions = edges.map(size => PKGraphPartitionBuilder.build(2, size, 1.0f, 0.0f))
  private lazy val k4Partitions = edges.map(size => PKGraphPartitionBuilder.build(4, size, 1.0f, 0.0f))
  private lazy val k8Partitions = edges.map(size => PKGraphPartitionBuilder.build(8, size, 1.0f, 0.0f))
  private lazy val k16Partitions = edges.map(size => PKGraphPartitionBuilder.build(16, size, 1.0f, 0.0f))
  private lazy val k32Partitions = edges.map(size => PKGraphPartitionBuilder.build(32, size, 1.0f, 0.0f))
  private lazy val k64Partitions = edges.map(size => PKGraphPartitionBuilder.build(64, size, 1.0f, 0.0f))

  performance of "IteratorBenchmark" in {
    /*using(graphXPartitions) curve "GraphX" in { partition =>
      val it = partition.iterator
      while (it.hasNext) {
        it.next()
      }
    }*/

    using(k2Partitions) curve "PKGraph (k=2)" in { partition =>
      val it = partition.tree.iterator
      while (it.hasNext) {
        it.next()
      }
    }

    using(k4Partitions) curve "PKGraph (k=4)" in { partition =>
      val it = partition.tree.iterator
      while (it.hasNext) {
        it.next()
      }
    }

    using(k8Partitions) curve "PKGraph (k=8)" in { partition =>
      val it = partition.tree.iterator
      while (it.hasNext) {
        it.next()
      }
    }

    using(k16Partitions) curve "PKGraph (k=16)" in { partition =>
      val it = partition.tree.iterator
      while (it.hasNext) {
        it.next()
      }
    }

    using(k32Partitions) curve "PKGraph (k=32)" in { partition =>
      val it = partition.tree.iterator
      while (it.hasNext) {
        it.next()
      }
    }

    using(k64Partitions) curve "PKGraph (k=64)" in { partition =>
      val it = partition.tree.iterator
      while (it.hasNext) {
        it.next()
      }
    }
  }
}
