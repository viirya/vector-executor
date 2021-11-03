package org.viirya.vector.bin

import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector
import org.apache.spark.sql.types._

import org.viirya.vector.native.VectorLib

object TestVector {
  def main(args: Array[String]): Unit = {
    System.loadLibrary("vector_lib")

    val lib = new VectorLib
    val integer = lib.test("123")
    println(s"integer: $integer")

    val schema = Seq(StructField("a", IntegerType))
    val vector = OffHeapColumnVector.allocateColumns(10, schema.toArray)(0)
    vector.putInt(0, 456)
    val vectorAddress = vector.valuesNativeAddress()
    val returned = lib.passOffHeapVector(vectorAddress, 10)
    println(s"returned: $returned")
  }
}