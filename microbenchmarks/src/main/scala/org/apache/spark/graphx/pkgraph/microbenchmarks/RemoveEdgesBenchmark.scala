package org.apache.spark.graphx.pkgraph.microbenchmarks

import org.apache.spark.graphx.pkgraph.microbenchmarks.builders.PKGraphPartitionBuilder
import org.scalameter.api.Gen

object RemoveEdgesBenchmark extends Microbenchmark("PKGraph (k=2)") {
  private lazy val edges: Gen[Int] = Gen.range("edges")(100, 1000, 100)

  private val density = 1.0f
  private val fixedSize = 100000

  private lazy val k2Partition = PKGraphPartitionBuilder.build(2, fixedSize, density, 0.0f)
  private lazy val k4Partition = PKGraphPartitionBuilder.build(2, fixedSize, density, 0.0f)
  private lazy val k8Partition = PKGraphPartitionBuilder.build(2, fixedSize, density, 0.0f)

  performance of "RemoveEdgesBenchmark" in {
    using(edges) curve "PKGraph (k=2)" in { size =>
      val deletedEdges = (0 until size).map(i => (i.toLong, i.toLong))
      k2Partition.removeEdges(deletedEdges.iterator)
    }

    using(edges) curve "PKGraph (k=4)" in { size =>
      val deletedEdges = (0 until size).map(i => (i.toLong, i.toLong))
      k4Partition.removeEdges(deletedEdges.iterator)
    }

    using(edges) curve "PKGraph (k=8)" in { size =>
      val deletedEdges = (0 until size).map(i => (i.toLong, i.toLong))
      k8Partition.removeEdges(deletedEdges.iterator)
    }
  }
}
