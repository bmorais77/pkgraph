package org.apache.spark.graphx

import org.apache.spark.graphx.impl.{ShippableVertexPartition, VertexAttributeBlock}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object vertices {

  /**
    * This implicit class is used to make some methods of VertexRDD public instead of package-private.
    * This is done to avoid having to copy all of the classes involved in implementing a VertexRDDImpl
    *
    * @param rdd [[VertexRDD]]
    * @tparam V Type of vertex attributes
    */
  implicit class PKVertexRDD[V](rdd: VertexRDD[V]) {

    /**
      * Applies a function to each `VertexPartition` of this RDD and returns a new VertexRDD.
      */
    def mapPKVertexPartitions[V2: ClassTag](
        f: ShippableVertexPartition[V] => ShippableVertexPartition[V2]
    ): VertexRDD[V2] = {
      rdd.mapVertexPartitions(f)
    }

    /** Generates an RDD of vertex attributes suitable for shipping to the edge partitions. */
    def shipPKVertexAttributes(shipSrc: Boolean, shipDst: Boolean): RDD[(PartitionID, VertexAttributeBlock[V])] = {
      rdd.shipVertexAttributes(shipSrc, shipDst)
    }
  }
}
