package org.apache.spark.graphx.pkgraph.benchmarks

import org.scalameter.api._
import org.scalameter.Measurer

object EdgePartitionMemoryBenchmark extends Bench.OfflineReport {
  override def measurer: Measurer[Double] = new Measurer.MemoryFootprint

  performance of "Sparse20" in {
    performance of "build" in {
      using(EdgePartitionDataSet.graphX20SparsePartitions) curve "GraphX" in { partition =>
        partition
      }

      using(EdgePartitionDataSet.k2PKGraph20SparsePartitions) curve "PKGraph (k=2)" in { partition =>
        partition
      }

      using(EdgePartitionDataSet.k4PKGraph20SparsePartitions) curve "PKGraph (k=4)" in { partition =>
        partition
      }

      using(EdgePartitionDataSet.k8PKGraph20SparsePartitions) curve "PKGraph (k=8)" in { partition =>
        partition
      }
    }
  }

  performance of "Sparse40" in {
    performance of "build" in {
      using(EdgePartitionDataSet.graphX20SparsePartitions) curve "GraphX" in { partition =>
        partition
      }

      using(EdgePartitionDataSet.k2PKGraph40SparsePartitions) curve "PKGraph (k=2)" in { partition =>
        partition
      }

      using(EdgePartitionDataSet.k4PKGraph40SparsePartitions) curve "PKGraph (k=4)" in { partition =>
        partition
      }

      using(EdgePartitionDataSet.k8PKGraph40SparsePartitions) curve "PKGraph (k=8)" in { partition =>
        partition
      }
    }
  }

  performance of "Sparse60" in {
    performance of "build" in {
      using(EdgePartitionDataSet.graphX60SparsePartitions) curve "GraphX" in { partition =>
        partition
      }

      using(EdgePartitionDataSet.k2PKGraph60SparsePartitions) curve "PKGraph (k=2)" in { partition =>
        partition
      }

      using(EdgePartitionDataSet.k4PKGraph60SparsePartitions) curve "PKGraph (k=4)" in { partition =>
        partition
      }

      using(EdgePartitionDataSet.k8PKGraph60SparsePartitions) curve "PKGraph (k=8)" in { partition =>
        partition
      }
    }
  }

  performance of "Sparse80" in {
    performance of "build" in {
      using(EdgePartitionDataSet.graphX80SparsePartitions) curve "GraphX" in { partition =>
        partition
      }

      using(EdgePartitionDataSet.k2PKGraph80SparsePartitions) curve "PKGraph (k=2)" in { partition =>
        partition
      }

      using(EdgePartitionDataSet.k4PKGraph80SparsePartitions) curve "PKGraph (k=4)" in { partition =>
        partition
      }

      using(EdgePartitionDataSet.k8PKGraph80SparsePartitions) curve "PKGraph (k=8)" in { partition =>
        partition
      }
    }
  }

  performance of "Full" in {
    performance of "build" in {
      using(EdgePartitionDataSet.graphXFullPartitions) curve "GraphX" in { partition =>
        partition
      }

      using(EdgePartitionDataSet.k2PKGraphFullPartitions) curve "PKGraph (k=2)" in { partition =>
        partition
      }

      using(EdgePartitionDataSet.k4PKGraphFullPartitions) curve "PKGraph (k=4)" in { partition =>
        partition
      }

      using(EdgePartitionDataSet.k8PKGraphFullPartitions) curve "PKGraph (k=8)" in { partition =>
        partition
      }
    }
  }
}
