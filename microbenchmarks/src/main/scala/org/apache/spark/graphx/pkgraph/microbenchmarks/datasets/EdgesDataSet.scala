package org.apache.spark.graphx.pkgraph.microbenchmarks.datasets

import org.scalameter.api.Gen

object EdgesDataSet {
  lazy val edges: Gen[Int] = Gen.range("edges")(100000, 1000000, 100000)
}
