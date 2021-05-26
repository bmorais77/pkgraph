name := "PKGraph"

version := "0.1"

scalaVersion := "2.12.10"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/releases"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.0"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.1.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test
libraryDependencies += "com.storm-enroute" %% "scalameter" % "0.19" % Test

// Spark uses a different version of jackson than Scalameter
dependencyOverrides += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.9"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.9"

testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")
parallelExecution in Test := false
