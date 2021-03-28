package org.apache.spark.graphx.pkgraph.graph

import org.apache.spark.graphx.{EdgeContext, EdgeDirection, Graph, TripletFields, VertexRDD}

import scala.reflect.ClassTag

abstract class PKGraph[V: ClassTag, E: ClassTag] extends Graph[V, E] {

  /**
    * Package-private feature that is leaked in the public interface.
    * Should not be used outside of GraphX.
    *
    * @return [[NotImplementedError]]
    */
  override def aggregateMessagesWithActiveSet[A: ClassTag](
      sendMsg: EdgeContext[V, E, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields,
      activeSetOpt: Option[(VertexRDD[_], EdgeDirection)]
  ): VertexRDD[A] = throw new NotImplementedError
}

object PKGraph {
  // TODO: Add factory methods for default implementation
}
