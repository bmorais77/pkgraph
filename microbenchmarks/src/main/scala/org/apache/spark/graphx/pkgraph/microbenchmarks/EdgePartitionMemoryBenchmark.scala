package org.apache.spark.graphx.pkgraph.microbenchmarks

import org.apache.spark.graphx.pkgraph.microbenchmarks.datasets.{GraphXDataSet, PKGraphDataSet}
import org.scalameter.api._
import org.scalameter.Measurer

object EdgePartitionMemoryBenchmark extends Bench.OfflineReport {
  override def measurer: Measurer[Double] = new Measurer.MemoryFootprint

  def performanceOf(testName: String, sparsity: Float): Unit = {
    performance of testName in {
      performance of "build" in {
        using(GraphXDataSet.buildPartitions(sparsity)) curve "GraphX" in { partition =>
          partition
        }

        using(PKGraphDataSet.buildPartitions(2, sparsity)) curve "PKGraph (k=2)" in { partition =>
          partition
        }

        using(PKGraphDataSet.buildPartitions(4, sparsity)) curve "PKGraph (k=4)" in { partition =>
          partition
        }

        using(PKGraphDataSet.buildPartitions(8, sparsity)) curve "PKGraph (k=8)" in { partition =>
          partition
        }
      }
    }
  }

  performanceOf("Dense", 0.0f)
  performanceOf("Sparse20", 0.2f)
  performanceOf("Sparse40", 0.4f)
  performanceOf("Sparse60", 0.6f)
  performanceOf("Sparse80", 0.8f)
}
