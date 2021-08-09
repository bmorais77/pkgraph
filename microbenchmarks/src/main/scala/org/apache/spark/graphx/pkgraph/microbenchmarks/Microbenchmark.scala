package org.apache.spark.graphx.pkgraph.microbenchmarks

import org.scalameter.api.{Bench, PGFPlotsReporter, Reporter}

abstract class Microbenchmark(referenceCurve: String) extends Bench.OfflineReport {
  override def reporter: Reporter[Double] =
    Reporter.Composite(super.reporter, PGFPlotsReporter(referenceCurve = referenceCurve))
}
