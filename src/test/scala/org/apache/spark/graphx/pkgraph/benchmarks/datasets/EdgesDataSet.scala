package org.apache.spark.graphx.pkgraph.benchmarks.datasets

import org.scalameter.api.Gen

object EdgesDataSet {
  lazy val edges: Gen[Int] = Gen.range("edges")(10000, 100000, 10000)
}
