package org.apache.spark.graphx.pkgraph.microbenchmarks

import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.pkgraph.microbenchmarks.datasets.PKGraphDataSet
import org.scalameter.api._

object MutableEdgePartitionBenchmark extends Bench.OfflineReport {
  private lazy val edges: Gen[Int] = Gen.range("edges")(10, 100, 10)

  def performanceOf(testName: String, sparsity: Float): Unit = {
    val k2Partition = PKGraphDataSet.buildPKGraphEdgePartition(2, 100 * 100, sparsity)
    val k4Partition = PKGraphDataSet.buildPKGraphEdgePartition(2, 100 * 100, sparsity)
    val k8Partition = PKGraphDataSet.buildPKGraphEdgePartition(2, 100 * 100, sparsity)

    performance of testName in {
      measure method "addEdges" in {
        using(edges) curve "PKGraph (k=2)" in { size =>
          val newEdges = (0 until size).map(i => new Edge[Int](i / size, i % size, i * 10))
          k2Partition.addEdges(newEdges.iterator)
        }

        using(edges) curve "PKGraph (k=4)" in { size =>
          val newEdges = (0 until size).map(i => new Edge[Int](i / size, i % size, i * 10))
          k4Partition.addEdges(newEdges.iterator)
        }

        using(edges) curve "PKGraph (k=8)" in { size =>
          val newEdges = (0 until size).map(i => new Edge[Int](i / size, i % size, i * 10))
          k8Partition.addEdges(newEdges.iterator)
        }
      }

      measure method "removeEdges" in {
        using(edges) curve "PKGraph (k=2)" in { size =>
          val deletedEdges = (0 until size).map(i => ((i / size).toLong, (i % size).toLong))
          k2Partition.removeEdges(deletedEdges.iterator)
        }

        using(edges) curve "PKGraph (k=4)" in { size =>
          val deletedEdges = (0 until size).map(i => ((i / size).toLong, (i % size).toLong))
          k4Partition.removeEdges(deletedEdges.iterator)
        }

        using(edges) curve "PKGraph (k=8)" in { size =>
          val deletedEdges = (0 until size).map(i => ((i / size).toLong, (i % size).toLong))
          k8Partition.removeEdges(deletedEdges.iterator)
        }
      }
    }
  }

  performanceOf("Dense", 0.0f)
  //performanceOf("Sparse20", 0.2f)
  //performanceOf("Sparse40", 0.4f)
  //performanceOf("Sparse60", 0.6f)
  //performanceOf("Sparse80", 0.8f)
}
