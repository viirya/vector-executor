package org.viirya.vector.native


class VectorLib {
  // Test native method
  @native def test(str: String): Int

  @native def passOffHeapVector(address: Long, numRows: Int): Int

  @native def projectOnVector(address: Long, numRows: Int, rightValue: Int): Int

  @native def projectOnTwoVectors(address1: Long, address2: Long, numRows: Int): Array[Long]

  @native def createPlan(plan: Array[Byte]): Long
  @native def getPlanString(plan: Long): String
  @native def getDeserializedPlan(plan: Array[Byte]): Array[Byte]
  @native def executePlan(plan: Long, addresses: Array[Long], numRows: Int): Array[Long]
  @native def releasePlan(plan: Long): Unit
}