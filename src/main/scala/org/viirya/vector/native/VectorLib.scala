package org.viirya.vector.native


class VectorLib {
  // Test native method
  @native def test(str: String): Int

  @native def passOffHeapVector(address: Long, numRows: Int): Int

  @native def projectOnVector(address: Long, numRows: Int, rightValue: Int): Int

  @native def projectOnTwoVectors(address1: Long, address2: Long, numRows: Int): Array[Long]
}