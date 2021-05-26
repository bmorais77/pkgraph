package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.pkgraph.util.SparkSessionTestWrapper
import org.apache.spark.storage.StorageLevel
import org.scalatest.FlatSpec

class PKEdgeRDDSpec extends FlatSpec with SparkSessionTestWrapper {
  private val edges: Seq[Edge[Int]] = (0 until 10).map(i => Edge(i, i, i * 10))

  "A PKEdgeRDD" should "build from edge RDD" in {
    val rdd = PKEdgeRDD.fromEdges(2, sc.parallelize(edges))
    assert(rdd.count() == edges.length)
  }

  it should "map values" in {
    val rdd = PKEdgeRDD.fromEdges(2, sc.parallelize(edges))
    val newRDD = rdd.mapValues(_.attr * 2)
    assert(newRDD.map(_.attr).collect() sameElements edges.map(_.attr * 2))
  }

  it should "reverse edges" in {
    val rdd = PKEdgeRDD.fromEdges(2, sc.parallelize(edges))
    val newRDD = rdd.reverse
    val expectedEdges = edges.map(e => Edge(e.dstId, e.srcId, e.attr)).toArray
    assert(newRDD.collect() sameElements expectedEdges)
  }

  it should "filter edges and vertices" in {
    val rdd = PKEdgeRDD.fromEdges[Int, Int](2, sc.parallelize(edges)).mapEdgePartitions { (_, partition) =>
      partition.updateVertices((0 until 10).map(i => (i.toLong, i * 100)).iterator)
    }
    val newRDD = rdd.filter(_.attr % 2 == 0, (id, _) => id % 2 == 0)

    val expectedEdges = edges.filter(e => e.attr % 2 == 0 && e.srcId % 2 == 0 && e.dstId % 2 == 0)
    val actualEdges = newRDD.collect()
    assert(actualEdges sameElements expectedEdges)
  }

  it should "inner join with another RDD" in {
    val rdd1 = PKEdgeRDD.fromEdges(2, sc.parallelize(edges.filter(_.srcId <= 6)))
    val rdd2 = PKEdgeRDD.fromEdges(2, sc.parallelize(edges.filter(_.srcId >= 4)))
    val newRDD = rdd1.innerJoin(rdd2) { (_, _, attr1, attr2) => attr1 + attr2 }

    val actualEdges = newRDD.map(_.attr).collect()
    val expectedEdges = edges.filter(e => e.srcId >= 4 && e.srcId <= 6).map(_.attr * 2)
    assert(actualEdges sameElements expectedEdges)
  }

  it should "map edge partitions" in {
    val rdd = PKEdgeRDD.fromEdges(2, sc.parallelize(edges))
    val newRDD = rdd.mapEdgePartitions { (_, partition) => partition.map(_.attr * 2) }
    assert(newRDD.map(_.attr).collect() sameElements edges.map(_.attr * 2))
  }

  it should "change the current storage level" in {
    val rdd = PKEdgeRDD.fromEdges(2, sc.parallelize(edges))
    val newRDD = rdd.withTargetStorageLevel(StorageLevel.DISK_ONLY)
    assert(newRDD.targetStorageLevel == StorageLevel.DISK_ONLY)
  }
}
