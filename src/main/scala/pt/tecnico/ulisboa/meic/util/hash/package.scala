package pt.tecnico.ulisboa.meic.util

package object hash {
  /**
   * Combines two integers using a Z-order curve (Morton Code).
   * The resulting code is an integer as only the first 16 bits of each
   * value are used.
   *
   * @param line 16-bit integer
   * @param col 16-bit integer
   * @return Morton code of 32-bit integer
   */
  def mortonCode(line: Int, col: Int): Int = {
    var code = 0
    for (i <- 0 until 16) {
      val mask = 0x01 << i
      val x = (line & mask) << 1
      val y = col & mask
      code |= (x | y) << i
    }
    code
  }
}
