package org.apache.spark.graphx.pkgraph.microbenchmarks

import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.pkgraph.microbenchmarks.builders.PKGraphPartitionBuilder
import org.scalameter.api.Gen

import scala.util.Random

object AddEdgesBenchmark extends Microbenchmark("PKGraph (k=2)") {
  private lazy val edges: Gen[Int] = Gen.range("edges")(10000, 100000, 10000)

  private val density = 1.0f
  private val fixedSize = 1000000
  private val fixedMatrixSize = math.floor(math.sqrt(fixedSize)).toInt

  private lazy val k2Partition = PKGraphPartitionBuilder.build(2, fixedSize, density, 0.0f)
  private lazy val k4Partition = PKGraphPartitionBuilder.build(4, fixedSize, density, 0.0f)
  private lazy val k8Partition = PKGraphPartitionBuilder.build(8, fixedSize, density, 0.0f)

  performance of "AddEdgesBenchmark" in {
    using(edges) curve "PKGraph (k=2)" in { size =>
      val newEdges =
        Seq.fill(size)(new Edge[Int](fixedMatrixSize + Random.nextInt(size), fixedMatrixSize + Random.nextInt(size), 1))
      k2Partition.addEdges(newEdges.iterator)
    }

    using(edges) curve "PKGraph (k=4)" in { size =>
      val newEdges =
        Seq.fill(size)(new Edge[Int](fixedMatrixSize + Random.nextInt(size), fixedMatrixSize + Random.nextInt(size), 1))
      k4Partition.addEdges(newEdges.iterator)
    }

    using(edges) curve "PKGraph (k=8)" in { size =>
      val newEdges =
        Seq.fill(size)(new Edge[Int](fixedMatrixSize + Random.nextInt(size), fixedMatrixSize + Random.nextInt(size), 1))
      k8Partition.addEdges(newEdges.iterator)
    }
  }
}
