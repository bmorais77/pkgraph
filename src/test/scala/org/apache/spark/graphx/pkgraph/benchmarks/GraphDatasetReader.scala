package org.apache.spark.graphx.pkgraph.benchmarks

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

/**
  * Each dataset should be contained in a folder with a "vertices.txt" file containing
  * the one vertex in each line and a "edges.txt" file containing one edge in each line.
  *
  * Each vertex is in the form:
  * id, attr
  *
  * Each edge is in the form:
  * srcId, dstId, attr
  *
  * The attribute value is always an integer.
  */
class GraphDatasetReader(spark: SparkSession) {
  import spark.implicits._

  private final val datasetsFolder: String = "src/test/resources/datasets"

  private final val vertexSchema = new StructType()
    .add(StructField("id", LongType, nullable = false))
    .add(StructField("attr", IntegerType, nullable = false))

  private final val edgeSchema = new StructType()
    .add(StructField("srcId", LongType, nullable = false))
    .add(StructField("dstId", LongType, nullable = false))
    .add(StructField("attr", IntegerType, nullable = false))

  def readDataset(dataset: String): Graph[Int, Int] = {
    val vertices = readVertices(s"$datasetsFolder/$dataset/vertices.txt")
    val edges = readEdges(s"$datasetsFolder/$dataset/edges.txt")
    Graph(vertices, edges)
  }

  private def readVertices(file: String): RDD[(VertexId, Int)] = {
    spark.read
      .schema(vertexSchema)
      .csv(file)
      .map { row =>
        val id = row.getAs[VertexId]("id")
        val attr = row.getAs[Int]("attr")
        (id, attr)
      }
      .rdd
  }

  private def readEdges(file: String): RDD[Edge[Int]] = {
    spark.read
      .schema(edgeSchema)
      .csv(file)
      .map { row =>
        val srcId = row.getAs[VertexId]("srcId")
        val dstId = row.getAs[VertexId]("dstId")
        val attr = row.getAs[Int]("attr")
        Edge(srcId, dstId, attr)
      }
      .rdd
  }
}
