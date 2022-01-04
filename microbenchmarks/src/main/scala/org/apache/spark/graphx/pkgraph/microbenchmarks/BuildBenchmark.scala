package org.apache.spark.graphx.pkgraph.microbenchmarks

import org.apache.spark.graphx.pkgraph.microbenchmarks.builders.{GraphXPartitionBuilder, PKGraphPartitionBuilder}
import org.scalameter.api.Gen

object BuildBenchmark extends Microbenchmark("PKGraph (k=2)") {
  private lazy val edges: Gen[Int] = Gen.range("edges")(100000, 1000000, 100000)

  performance of "BuildBenchmark" in {
    /*using(edges) curve "GraphX" in { size =>
      GraphXPartitionBuilder.build(size, 1.0f, 0.0f)
    }*/

    using(edges) curve "PKGraph (k=2)" in { size =>
      PKGraphPartitionBuilder.build(2, size, 1.0f, 0.0f)
    }

    using(edges) curve "PKGraph (k=4)" in { size =>
      PKGraphPartitionBuilder.build(4, size, 1.0f, 0.0f)
    }

    using(edges) curve "PKGraph (k=8)" in { size =>
      PKGraphPartitionBuilder.build(8, size, 1.0f, 0.0f)
    }

    using(edges) curve "PKGraph (k=16)" in { size =>
      PKGraphPartitionBuilder.build(16, size, 1.0f, 0.0f)
    }

    using(edges) curve "PKGraph (k=32)" in { size =>
      PKGraphPartitionBuilder.build(32, size, 1.0f, 0.0f)
    }

    using(edges) curve "PKGraph (k=64)" in { size =>
      PKGraphPartitionBuilder.build(64, size, 1.0f, 0.0f)
    }
  }
}
