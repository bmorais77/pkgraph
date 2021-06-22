package org.apache.spark.graphx.pkgraph

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark test example")
      .getOrCreate()
  }

  lazy val sc: SparkContext = spark.sparkContext
}
