name := "PKGraph"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.0"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.1.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test

