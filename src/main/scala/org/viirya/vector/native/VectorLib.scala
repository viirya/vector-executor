package org.viirya.vector.native


class VectorLib {
  // Test native method
  @native def test(str: String): Int

  @native def passOffHeapVector(address: Long, rowNum: Int): Int
}