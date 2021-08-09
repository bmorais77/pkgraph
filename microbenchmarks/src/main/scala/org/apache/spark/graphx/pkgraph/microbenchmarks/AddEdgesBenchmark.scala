package org.apache.spark.graphx.pkgraph.microbenchmarks

import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.pkgraph.microbenchmarks.builders.PKGraphPartitionBuilder
import org.scalameter.api.Gen

object AddEdgesBenchmark extends Microbenchmark("PKGraph (k=2)") {
  private lazy val edges: Gen[Int] = Gen.range("edges")(100, 1000, 100)

  private val density = 1.0f
  private val fixedSize = 100000

  private lazy val k2Partition = PKGraphPartitionBuilder.build(2, fixedSize, density, 0.0f)
  private lazy val k4Partition = PKGraphPartitionBuilder.build(4, fixedSize, density, 0.0f)
  private lazy val k8Partition = PKGraphPartitionBuilder.build(8, fixedSize, density, 0.0f)

  performance of "AddEdgesBenchmark" in {
    using(edges) curve "PKGraph (k=2)" in { size =>
      val newEdges = (0 until size).map(i => new Edge[Int](i, i, i * 10))
      k2Partition.addEdges(newEdges.iterator)
    }

    using(edges) curve "PKGraph (k=4)" in { size =>
      val newEdges = (0 until size).map(i => new Edge[Int](i, i, i * 10))
      k4Partition.addEdges(newEdges.iterator)
    }

    using(edges) curve "PKGraph (k=8)" in { size =>
      val newEdges = (0 until size).map(i => new Edge[Int](i, i, i * 10))
      k8Partition.addEdges(newEdges.iterator)
    }
  }
}
