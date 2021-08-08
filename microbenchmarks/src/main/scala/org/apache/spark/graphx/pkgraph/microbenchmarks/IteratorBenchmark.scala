package org.apache.spark.graphx.pkgraph.microbenchmarks

import org.apache.spark.graphx.pkgraph.microbenchmarks.builders.{GraphXPartitionBuilder, PKGraphPartitionBuilder}
import org.scalameter.api.Gen

object IteratorBenchmark extends Microbenchmark("GraphX") {
  private lazy val edges: Gen[Int] = Gen.range("edges")(100000, 1000000, 100000)

  private val graphXPartitions = edges.map(size => GraphXPartitionBuilder.build(size, 1.0f, 0.0f))
  private val k2Partitions = edges.map(size => PKGraphPartitionBuilder.build(2, size, 1.0f, 0.0f))
  private val k4Partitions = edges.map(size => PKGraphPartitionBuilder.build(4, size, 1.0f, 0.0f))
  private val k8Partitions = edges.map(size => PKGraphPartitionBuilder.build(8, size, 1.0f, 0.0f))

  performance of "IteratorBenchmark" in {
    using(graphXPartitions) curve "GraphX" in { partition =>
      val it = partition.iterator
      while (it.hasNext) {
        it.next()
      }
    }

    using(k2Partitions) curve "PKGraph (k=2) (iterator)" in { partition =>
      val it = partition.tree.iterator
      while (it.hasNext) {
        it.next()
      }
    }

    using(k4Partitions) curve "PKGraph (k=4) (iterator)" in { partition =>
      val it = partition.tree.iterator
      while (it.hasNext) {
        it.next()
      }
    }

    using(k8Partitions) curve "PKGraph (k=8) (iterator)" in { partition =>
      val it = partition.tree.iterator
      while (it.hasNext) {
        it.next()
      }
    }
  }
}
