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

    // Project(add(vector, val))
    var added = lib.projectOnVector(vectorAddress, 10, 5)
    // 456 + 5 = 461
    println(s"returned vector[0]: $added")

    val anotherVector = OffHeapColumnVector.allocateColumns(10, schema.toArray)(0)
    anotherVector.putInt(0, 123)
    val anotherVectorAddress = anotherVector.valuesNativeAddress()

    // Project(add(vector, vector))
    added = lib.projectOnTwoVectors(vectorAddress, anotherVectorAddress, 10)
    // 456 + 123 = 579
    println(s"returned vector[0]: $added")
  }
}