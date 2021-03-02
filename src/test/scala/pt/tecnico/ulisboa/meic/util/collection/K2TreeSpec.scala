package pt.tecnico.ulisboa.meic.util.collection

import org.scalatest.FlatSpec

class K2TreeSpec extends FlatSpec {
  "A K²-Tree" should "build from edge list" in {
    val edges = Array((0, 3), (1, 0), (1,1), (2,1), (3,0), (3,3))
    val tree = K2Tree(2, 4, edges)
    assert(tree.k == 2)
  }
}
