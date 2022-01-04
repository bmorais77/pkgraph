package org.apache.spark.graphx.pkgraph.microbenchmarks

import org.apache.spark.graphx.pkgraph.microbenchmarks.builders.{GraphXPartitionBuilder, PKGraphPartitionBuilder}
import org.scalameter.Measurer
import org.scalameter.api.Gen

object MemoryWithPoorLocalityBenchmark extends Microbenchmark("GraphX") {
  override def measurer: Measurer[Double] = new Measurer.MemoryFootprint

  private val fixedSize = 1000000
  private lazy val densities: Gen[Float] = Gen
    .range("density")(10, 100, 10)
    .map(i => i.toFloat / 100f)

  performance of "MemoryWithPoorLocalityBenchmark" in {
    measure method "build" in {
      using(densities) curve "GraphX" in { density =>
        GraphXPartitionBuilder.build(fixedSize, density, 0.0f)
      }

      using(densities) curve "PKGraph (k=2)" in { density =>
        PKGraphPartitionBuilder.build(2, fixedSize, density, 0.0f)
      }

      using(densities) curve "PKGraph (k=4)" in { density =>
        PKGraphPartitionBuilder.build(4, fixedSize, density, 0.0f)
      }

      using(densities) curve "PKGraph (k=8)" in { density =>
        PKGraphPartitionBuilder.build(8, fixedSize, density, 0.0f)
      }

      using(densities) curve "PKGraph (k=16)" in { density =>
        PKGraphPartitionBuilder.build(16, fixedSize, density, 0.0f)
      }

      using(densities) curve "PKGraph (k=32)" in { density =>
        PKGraphPartitionBuilder.build(32, fixedSize, density, 0.0f)
      }

      using(densities) curve "PKGraph (k=64)" in { density =>
        PKGraphPartitionBuilder.build(64, fixedSize, density, 0.0f)
      }

      using(densities) curve "PKGraph (k=128)" in { density =>
        PKGraphPartitionBuilder.build(128, fixedSize, density, 0.0f)
      }
    }
  }
}
