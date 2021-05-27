package org.apache.spark.graphx.pkgraph.benchmarks

import org.scalameter.api._

object EdgePartitionBenchmark extends Bench.OfflineReport {
  performance of "Edge Partition" in {
    performance of "Sparse (20%)" in {
      measure method "map" in {
        using(EdgePartitionDataSet.graphX20SparsePartitions) curve "GraphX" in {
          _.map(e => e.attr * 2)
        }

        using(EdgePartitionDataSet.k2PKGraph20SparsePartitions) curve "PKGraph (k=2)" in {
          _.map(e => e.attr * 2)
        }

        using(EdgePartitionDataSet.k4PKGraph20SparsePartitions) curve "PKGraph (k=4)" in {
          _.map(e => e.attr * 2)
        }

        using(EdgePartitionDataSet.k8PKGraph20SparsePartitions) curve "PKGraph (k=8)" in {
          _.map(e => e.attr * 2)
        }
      }
    }

    performance of "Sparse (40%)" in {
      measure method "map" in {
        using(EdgePartitionDataSet.graphX40SparsePartitions) curve "GraphX" in {
          _.map(e => e.attr * 2)
        }

        using(EdgePartitionDataSet.k2PKGraph40SparsePartitions) curve "PKGraph (k=2)" in {
          _.map(e => e.attr * 2)
        }

        using(EdgePartitionDataSet.k4PKGraph40SparsePartitions) curve "PKGraph (k=4)" in {
          _.map(e => e.attr * 2)
        }

        using(EdgePartitionDataSet.k8PKGraph40SparsePartitions) curve "PKGraph (k=8)" in {
          _.map(e => e.attr * 2)
        }
      }
    }

    performance of "Sparse (60%)" in {
      measure method "map" in {
        using(EdgePartitionDataSet.graphX60SparsePartitions) curve "GraphX" in {
          _.map(e => e.attr * 2)
        }

        using(EdgePartitionDataSet.k2PKGraph60SparsePartitions) curve "PKGraph (k=2)" in {
          _.map(e => e.attr * 2)
        }

        using(EdgePartitionDataSet.k4PKGraph60SparsePartitions) curve "PKGraph (k=4)" in {
          _.map(e => e.attr * 2)
        }

        using(EdgePartitionDataSet.k8PKGraph60SparsePartitions) curve "PKGraph (k=8)" in {
          _.map(e => e.attr * 2)
        }
      }
    }

    performance of "Sparse (80%)" in {
      measure method "map" in {
        using(EdgePartitionDataSet.graphX80SparsePartitions) curve "GraphX" in {
          _.map(e => e.attr * 2)
        }

        using(EdgePartitionDataSet.k2PKGraph80SparsePartitions) curve "PKGraph (k=2)" in {
          _.map(e => e.attr * 2)
        }

        using(EdgePartitionDataSet.k4PKGraph80SparsePartitions) curve "PKGraph (k=4)" in {
          _.map(e => e.attr * 2)
        }

        using(EdgePartitionDataSet.k8PKGraph80SparsePartitions) curve "PKGraph (k=8)" in {
          _.map(e => e.attr * 2)
        }
      }
    }

    performance of "Full" in {
      measure method "map" in {
        using(EdgePartitionDataSet.graphXFullPartitions) curve "GraphX" in {
          _.map(e => e.attr * 2)
        }

        using(EdgePartitionDataSet.k2PKGraphFullPartitions) curve "PKGraph (k=2)" in {
          _.map(e => e.attr * 2)
        }

        using(EdgePartitionDataSet.k4PKGraphFullPartitions) curve "PKGraph (k=4)" in {
          _.map(e => e.attr * 2)
        }

        using(EdgePartitionDataSet.k8PKGraphFullPartitions) curve "PKGraph (k=8)" in {
          _.map(e => e.attr * 2)
        }
      }
    }
  }
}
