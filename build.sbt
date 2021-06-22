import sbt.Keys.{dependencyOverrides, libraryDependencies}

lazy val settings = Seq(
  organization := "org.apache.spark.graphx.pkgraph",
  version := "0.0.1",
  scalaVersion := "2.12.10"
)

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
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.0",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.0",
    libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.1.0",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test
  )

lazy val microbenchmarks = project
  .settings(
    name := "microbenchmarks",
    settings,
    resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/releases",
    libraryDependencies += "com.storm-enroute" %% "scalameter" % "0.19",
    dependencyOverrides += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.9",
    dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.9"
  )
  .dependsOn(pkgraph)

lazy val macrobenchmarks = project
  .settings(
    name := "macrobenchmarks",
    settings
  )
  .dependsOn(pkgraph)
