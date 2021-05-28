package org.apache.spark.graphx.pkgraph.benchmarks

import org.scalameter.api._

object EdgePartitionBenchmark extends Bench.OfflineReport {
  performance of "Sparse20" in {
    measure method "build" in {
      using(EdgePartitionDataSet.partitionSizes) curve "GraphX" in { size =>
        EdgePartitionDataSet.buildGraphXEdgePartition(size, 0.2f)
      }

      using(EdgePartitionDataSet.partitionSizes) curve "PKGraph (k=2)" in { size =>
        EdgePartitionDataSet.buildPKGraphEdgePartition(2, size, 0.2f)
      }

      using(EdgePartitionDataSet.partitionSizes) curve "PKGraph (k=4)" in { size =>
        EdgePartitionDataSet.buildPKGraphEdgePartition(4, size, 0.2f)
      }

      using(EdgePartitionDataSet.partitionSizes) curve "PKGraph (k=8)" in { size =>
        EdgePartitionDataSet.buildPKGraphEdgePartition(8, size, 0.2f)
      }
    }

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

    measure method "filter" in {
      using(EdgePartitionDataSet.graphX20SparsePartitionsWithVertices) curve "GraphX" in {
        _.filter(_ => true, (_, attr) => attr % 2 == 0)
      }

      using(EdgePartitionDataSet.k2PKGraph20SparsePartitionsWithVertices) curve "PKGraph (k=2)" in {
        _.filter(_ => true, (_, attr) => attr % 2 == 0)
      }

      using(EdgePartitionDataSet.k4PKGraph20SparsePartitionsWithVertices) curve "PKGraph (k=4)" in {
        _.filter(_ => true, (_, attr) => attr % 2 == 0)
      }

      using(EdgePartitionDataSet.k8PKGraph20SparsePartitionsWithVertices) curve "PKGraph (k=8)" in {
        _.filter(_ => true, (_, attr) => attr % 2 == 0)
      }
    }
  }

  performance of "Sparse40" in {
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

    measure method "filter" in {
      using(EdgePartitionDataSet.graphX40SparsePartitionsWithVertices) curve "GraphX" in {
        _.filter(_ => true, (_, attr) => attr % 2 == 0)
      }

      using(EdgePartitionDataSet.k2PKGraph40SparsePartitionsWithVertices) curve "PKGraph (k=2)" in {
        _.filter(_ => true, (_, attr) => attr % 2 == 0)
      }

      using(EdgePartitionDataSet.k4PKGraph40SparsePartitionsWithVertices) curve "PKGraph (k=4)" in {
        _.filter(_ => true, (_, attr) => attr % 2 == 0)
      }

      using(EdgePartitionDataSet.k8PKGraph40SparsePartitionsWithVertices) curve "PKGraph (k=8)" in {
        _.filter(_ => true, (_, attr) => attr % 2 == 0)
      }
    }
  }

  performance of "Sparse60" in {
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

    measure method "filter" in {
      using(EdgePartitionDataSet.graphX60SparsePartitionsWithVertices) curve "GraphX" in {
        _.filter(_ => true, (_, attr) => attr % 2 == 0)
      }

      using(EdgePartitionDataSet.k2PKGraph60SparsePartitionsWithVertices) curve "PKGraph (k=2)" in {
        _.filter(_ => true, (_, attr) => attr % 2 == 0)
      }

      using(EdgePartitionDataSet.k4PKGraph60SparsePartitionsWithVertices) curve "PKGraph (k=4)" in {
        _.filter(_ => true, (_, attr) => attr % 2 == 0)
      }

      using(EdgePartitionDataSet.k8PKGraph60SparsePartitionsWithVertices) curve "PKGraph (k=8)" in {
        _.filter(_ => true, (_, attr) => attr % 2 == 0)
      }
    }
  }

  performance of "Sparse80" in {
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

    measure method "filter" in {
      using(EdgePartitionDataSet.graphX60SparsePartitionsWithVertices) curve "GraphX" in {
        _.filter(_ => true, (_, attr) => attr % 2 == 0)
      }

      using(EdgePartitionDataSet.k2PKGraph60SparsePartitionsWithVertices) curve "PKGraph (k=2)" in {
        _.filter(_ => true, (_, attr) => attr % 2 == 0)
      }

      using(EdgePartitionDataSet.k4PKGraph60SparsePartitionsWithVertices) curve "PKGraph (k=4)" in {
        _.filter(_ => true, (_, attr) => attr % 2 == 0)
      }

      using(EdgePartitionDataSet.k8PKGraph60SparsePartitionsWithVertices) curve "PKGraph (k=8)" in {
        _.filter(_ => true, (_, attr) => attr % 2 == 0)
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

    measure method "filter" in {
      using(EdgePartitionDataSet.graphXFullPartitionsWithVertices) curve "GraphX" in {
        _.filter(_ => true, (_, attr) => attr % 2 == 0)
      }

      using(EdgePartitionDataSet.k2PKGraphFullPartitionsWithVertices) curve "PKGraph (k=2)" in {
        _.filter(_ => true, (_, attr) => attr % 2 == 0)
      }

      using(EdgePartitionDataSet.k4PKGraphFullPartitionsWithVertices) curve "PKGraph (k=4)" in {
        _.filter(_ => true, (_, attr) => attr % 2 == 0)
      }

      using(EdgePartitionDataSet.k8PKGraphFullPartitionsWithVertices) curve "PKGraph (k=8)" in {
        _.filter(_ => true, (_, attr) => attr % 2 == 0)
      }
    }
  }
}
