package pt.tecnico.ulisboa.meic

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession;

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Simple Application")
      .master("local")
      .getOrCreate()

    val result = run(spark.sparkContext)
    println("###########################################################")
    println("RESULT:")
    println(result)
    println("###########################################################")
    spark.stop()
  }

  def run(sc: SparkContext): String = {
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Seq((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

    val edges: RDD[Edge[String]] =
      sc.parallelize(Seq(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    val defaultUser = ("John Doe", "Missing")
    val graph = Graph(users, edges, defaultUser)

    val neighbors = graph.collectNeighbors(EdgeDirection.Out).collect()
    val result = new StringBuilder()
    for (neighbor <- neighbors) {
      result ++= s"${neighbor._1} -> ${neighbor._2.map(i => i._1).mkString("[", ", ", "]")}\n"
    }
    result.toString()
  }
}
