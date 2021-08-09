package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.graphx.{PartitionID, PartitionStrategy, VertexId}

/**
 * Partitions the global adjacency matrix into several 'blocks' of roughly equal size.
 *
 * @param k             Value of the K²-Tree that will be used
 * @param matrixSize    Size of the global matrix
 */
class PKGridPartitionStrategy(matrixSize: Int) extends PartitionStrategy {
  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    val ceilSqrtNumParts: PartitionID = math.ceil(math.sqrt(numParts)).toInt
    val blockSize = matrixSize / ceilSqrtNumParts
    val maxLineAndCol = matrixSize / blockSize - 1
    val row: PartitionID = math.min(math.abs(src) / blockSize, maxLineAndCol).toInt
    val col: PartitionID = math.min(math.abs(dst) / blockSize, maxLineAndCol).toInt
    (row * ceilSqrtNumParts + col) % numParts
  }
}
