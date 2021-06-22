package org.apache.spark.graphx.pkgraph.util

package object mathx {
  /**
   * Performs the logarithmic operation with the given base and value.
   *
   * log<base>(x) = log10(x) / log10(base)
   *
   * @param base Base
   * @param x Value
   * @return logarithmic result
   */
  def log(base: Double, x: Double): Double = math.log10(x) / math.log10(base)
}
