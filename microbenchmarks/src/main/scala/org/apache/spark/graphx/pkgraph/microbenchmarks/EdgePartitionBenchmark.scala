package org.apache.spark.graphx.pkgraph.microbenchmarks

import org.apache.spark.graphx.TripletFields
import org.apache.spark.graphx.impl.EdgeActiveness
import org.apache.spark.graphx.pkgraph.microbenchmarks.datasets.{EdgesDataSet, GraphXDataSet, PKGraphDataSet}
import org.scalameter.api._

object EdgePartitionBenchmark extends Bench.OfflineReport {
  def performanceOf(testName: String, density: Float): Unit = {
    performance of testName in {
      measure method "build" in {
        using(EdgesDataSet.edges) curve "GraphX" in { size =>
          GraphXDataSet.buildGraphXEdgePartition(size, density)
        }

        using(EdgesDataSet.edges) curve "PKGraph (k=2)" in { size =>
          PKGraphDataSet.buildPKGraphEdgePartition(2, size, density)
        }

        using(EdgesDataSet.edges) curve "PKGraph (k=4)" in { size =>
          PKGraphDataSet.buildPKGraphEdgePartition(4, size, density)
        }

        using(EdgesDataSet.edges) curve "PKGraph (k=8)" in { size =>
          PKGraphDataSet.buildPKGraphEdgePartition(8, size, density)
        }
      }

      measure method "iterator" in {
        using(GraphXDataSet.buildPartitions(density)) curve "GraphX" in { partition =>
          val it = partition.iterator
          while (it.hasNext) {
            it.next()
          }
        }

        using(PKGraphDataSet.buildPartitions(2, density)) curve "PKGraph (k=2) (recursive)" in { partition =>
          partition.tree.forEachEdge { (_, _) => }
        }

        using(PKGraphDataSet.buildPartitions(2, density)) curve "PKGraph (k=2) (iterator)" in { partition =>
          val it = partition.tree.iterator
          while (it.hasNext) {
            it.next()
          }
        }

        using(PKGraphDataSet.buildPartitions(4, density)) curve "PKGraph (k=4) (recursive)" in { partition =>
          partition.tree.forEachEdge { (_, _) => }
        }

        using(PKGraphDataSet.buildPartitions(4, density)) curve "PKGraph (k=4) (iterator)" in { partition =>
          val it = partition.tree.iterator
          while (it.hasNext) {
            it.next()
          }
        }

        using(PKGraphDataSet.buildPartitions(8, density)) curve "PKGraph (k=8) (recursive)" in { partition =>
          partition.tree.forEachEdge { (_, _) => }
        }

        using(PKGraphDataSet.buildPartitions(8, density)) curve "PKGraph (k=8) (iterator)" in { partition =>
          val it = partition.tree.iterator
          while (it.hasNext) {
            it.next()
          }
        }
      }

      measure method "map" in {
        using(GraphXDataSet.buildPartitions(density)) curve "GraphX" in {
          _.map(e => e.attr * 2)
        }

        using(PKGraphDataSet.buildPartitions(2, density)) curve "PKGraph (k=2)" in {
          _.map(e => e.attr * 2)
        }

        using(PKGraphDataSet.buildPartitions(4, density)) curve "PKGraph (k=4)" in {
          _.map(e => e.attr * 2)
        }

        using(PKGraphDataSet.buildPartitions(8, density)) curve "PKGraph (k=8)" in {
          _.map(e => e.attr * 2)
        }
      }

      measure method "filter" in {
        using(GraphXDataSet.buildPartitionsWithVertices(density)) curve "GraphX" in {
          _.filter(_ => true, (_, attr) => attr % 2 == 0)
        }

        using(PKGraphDataSet.buildPartitionsWithVertices(2, density)) curve "PKGraph (k=2)" in {
          _.filter(_ => true, (_, attr) => attr % 2 == 0)
        }

        using(PKGraphDataSet.buildPartitionsWithVertices(4, density)) curve "PKGraph (k=4)" in {
          _.filter(_ => true, (_, attr) => attr % 2 == 0)
        }

        using(PKGraphDataSet.buildPartitionsWithVertices(8, density)) curve "PKGraph (k=8)" in {
          _.filter(_ => true, (_, attr) => attr % 2 == 0)
        }
      }

      measure method "innerJoin" in {
        using(GraphXDataSet.buildPartitionsForInnerJoin(density)) curve "GraphX" in {
          case (p1, p2) =>
            p1.innerJoin(p2) { (_, _, attr1, attr2) => attr1 + attr2 }
        }

        using(PKGraphDataSet.buildPartitionsForInnerJoin(2, density)) curve "PKGraph (k=2)" in {
          case (p1, p2) =>
            p1.innerJoin(p2) { (_, _, attr1, attr2) => attr1 + attr2 }
        }

        using(PKGraphDataSet.buildPartitionsForInnerJoin(4, density)) curve "PKGraph (k=4)" in {
          case (p1, p2) =>
            p1.innerJoin(p2) { (_, _, attr1, attr2) => attr1 + attr2 }
        }

        using(PKGraphDataSet.buildPartitionsForInnerJoin(8, density)) curve "PKGraph (k=8)" in {
          case (p1, p2) =>
            p1.innerJoin(p2) { (_, _, attr1, attr2) => attr1 + attr2 }
        }
      }

      measure method "aggregateMessagesEdgeScan" in {
        using(GraphXDataSet.buildPartitionsWithVertices(density)) curve "GraphX" in { partition =>
          partition
            .aggregateMessagesEdgeScan[Int](ctx => ctx.sendToSrc(10), _ + _, TripletFields.All, EdgeActiveness.Neither)
        }

        using(PKGraphDataSet.buildPartitionsWithVertices(2, density)) curve "PKGraph (k=2)" in { partition =>
          partition
            .aggregateMessagesEdgeScan[Int](ctx => ctx.sendToSrc(10), _ + _, TripletFields.All, EdgeActiveness.Neither)
        }

        using(PKGraphDataSet.buildPartitionsWithVertices(4, density)) curve "PKGraph (k=4)" in { partition =>
          partition
            .aggregateMessagesEdgeScan[Int](ctx => ctx.sendToSrc(10), _ + _, TripletFields.All, EdgeActiveness.Neither)
        }

        using(PKGraphDataSet.buildPartitionsWithVertices(8, density)) curve "PKGraph (k=8)" in { partition =>
          partition
            .aggregateMessagesEdgeScan[Int](ctx => ctx.sendToSrc(10), _ + _, TripletFields.All, EdgeActiveness.Neither)
        }
      }

      measure method "aggregateMessagesIndexScan" in {
        using(GraphXDataSet.buildPartitionsWithActiveVertices(density)) curve "GraphX" in { partition =>
          partition
            .aggregateMessagesIndexScan[Int](ctx => ctx.sendToSrc(10), _ + _, TripletFields.All, EdgeActiveness.SrcOnly)
        }

        using(PKGraphDataSet.buildPartitionsWithActiveVertices(2, density)) curve "PKGraph (k=2) (edges)" in {
          partition =>
            partition.aggregateMessagesEdgeScan[Int](
              ctx => ctx.sendToSrc(10),
              _ + _,
              TripletFields.All,
              EdgeActiveness.SrcOnly
            )
        }

        using(PKGraphDataSet.buildPartitionsWithActiveVertices(2, density)) curve "PKGraph (k=2) (index)" in {
          partition =>
            partition.aggregateMessagesSrcIndexScan[Int](
              ctx => ctx.sendToSrc(10),
              _ + _,
              TripletFields.All,
              EdgeActiveness.SrcOnly
            )
        }

        using(PKGraphDataSet.buildPartitionsWithActiveVertices(4, density)) curve "PKGraph (k=4) (edges)" in {
          partition =>
            partition.aggregateMessagesEdgeScan[Int](
              ctx => ctx.sendToSrc(10),
              _ + _,
              TripletFields.All,
              EdgeActiveness.SrcOnly
            )
        }

        using(PKGraphDataSet.buildPartitionsWithActiveVertices(4, density)) curve "PKGraph (k=4) (index)" in {
          partition =>
            partition.aggregateMessagesSrcIndexScan[Int](
              ctx => ctx.sendToSrc(10),
              _ + _,
              TripletFields.All,
              EdgeActiveness.SrcOnly
            )
        }

        using(PKGraphDataSet.buildPartitionsWithActiveVertices(8, density)) curve "PKGraph (k=8) (edges)" in {
          partition =>
            partition.aggregateMessagesEdgeScan[Int](
              ctx => ctx.sendToSrc(10),
              _ + _,
              TripletFields.All,
              EdgeActiveness.SrcOnly
            )
        }

        using(PKGraphDataSet.buildPartitionsWithActiveVertices(8, density)) curve "PKGraph (k=8) (index)" in {
          partition =>
            partition.aggregateMessagesSrcIndexScan[Int](
              ctx => ctx.sendToSrc(10),
              _ + _,
              TripletFields.All,
              EdgeActiveness.SrcOnly
            )
        }
      }
    }
  }

  performanceOf("Dense", 0.0f)
  performanceOf("Sparse20", 0.2f)
  performanceOf("Sparse40", 0.4f)
  performanceOf("Sparse60", 0.6f)
  performanceOf("Sparse80", 0.8f)
}
