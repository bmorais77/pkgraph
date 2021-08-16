import sbt.Keys.{dependencyOverrides, libraryDependencies}

lazy val settings = Seq(
  organization := "org.apache.spark.graphx.pkgraph",
  version := "1.0.0",
  scalaVersion := "2.12.10"
)

val sparkVersion = "3.1.1"
lazy val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-graphx" % sparkVersion
)

// "provided" dependencies are not transitive so, even though the benchmarks depend on the pkgraph library,
// the spark dependencies need to be added to each project
lazy val providedSparkDependencies = sparkDependencies.map(moduleID => moduleID % "provided")

lazy val root = project
  .in(file("."))
  .settings(settings)
  .aggregate(
    pkgraph,
    microbenchmarks,
    macrobenchmarks
  )

lazy val pkgraph = project
  .settings(
    name := "pkgraph",
    settings,
    libraryDependencies ++= providedSparkDependencies,
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test
  )

lazy val microbenchmarks = project
  .settings(
    name := "microbenchmarks",
    settings,
    resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/releases",
    libraryDependencies ++= sparkDependencies,
    libraryDependencies += "com.storm-enroute" %% "scalameter" % "0.19",
    dependencyOverrides += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.9",
    dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.9"
  )
  .dependsOn(pkgraph)

lazy val macrobenchmarks = project
  .settings(
    name := "macrobenchmarks",
    settings,
    // Use 'providedSparkDependencies' when running the macrobenchmarks in a cluster
    //libraryDependencies ++= sparkDependencies,
    libraryDependencies ++= providedSparkDependencies,
    libraryDependencies += "ch.cern.sparkmeasure" %% "spark-measure" % "0.17"
  )
  .dependsOn(pkgraph)
