package org.apache.spark.graphx.pkgraph.microbenchmarks

import org.apache.spark.graphx.pkgraph.microbenchmarks.builders.PKGraphPartitionBuilder
import org.scalameter.api.Gen

import scala.util.Random

object RemoveEdgesBenchmark extends Microbenchmark("PKGraph (k=2)") {
  private lazy val edges: Gen[Int] = Gen.range("edges")(1000, 10000, 1000)

  private val density = 1.0f
  private val fixedSize = 1000000
  private val fixedMatrixSize = math.floor(math.sqrt(fixedSize)).toInt

  private lazy val k2Partition = PKGraphPartitionBuilder.build(2, fixedSize, density, 0.0f)
  private lazy val k4Partition = PKGraphPartitionBuilder.build(2, fixedSize, density, 0.0f)
  private lazy val k8Partition = PKGraphPartitionBuilder.build(2, fixedSize, density, 0.0f)

  performance of "RemoveEdgesBenchmark" in {
    using(edges) curve "PKGraph (k=2)" in { size =>
      val deletedEdges = Seq.fill(size)((Random.nextInt(fixedMatrixSize).toLong, Random.nextInt(fixedMatrixSize).toLong))
      k2Partition.removeEdges(deletedEdges.iterator)
    }

    using(edges) curve "PKGraph (k=4)" in { size =>
      val deletedEdges = Seq.fill(size)((Random.nextInt(fixedMatrixSize).toLong, Random.nextInt(fixedMatrixSize).toLong))
      k4Partition.removeEdges(deletedEdges.iterator)
    }

    using(edges) curve "PKGraph (k=8)" in { size =>
      val deletedEdges = Seq.fill(size)((Random.nextInt(fixedMatrixSize).toLong, Random.nextInt(fixedMatrixSize).toLong))
      k8Partition.removeEdges(deletedEdges.iterator)
    }
  }
}
