package org.viirya.vector.bin

import org.apache.arrow.ffi.FFI
import org.apache.arrow.ffi.ArrowArray
import org.apache.arrow.ffi.ArrowSchema
import org.apache.arrow.ffi.FFIDictionaryProvider
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BigIntVector
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.IntVector

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
    val array_addresses = lib.projectOnTwoVectors(vectorAddress, anotherVectorAddress, 10)
    read_arrow_arrays(array_addresses)
  }


  def read_arrow_arrays(arrayAddress: Array[Long]): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    val ffiDictionaryProvider = new FFIDictionaryProvider()

    for (i <- 0 until arrayAddress.length by 2) {
      println(s"arrow array: $i")

      val arrowSchema = ArrowSchema.wrap(arrayAddress(i + 1))
      val arrowArray = ArrowArray.wrap(arrayAddress(i))
      val imported = FFI.importVector(allocator, arrowArray, arrowSchema, ffiDictionaryProvider)

      val rowCount = imported.getValueCount
      println("rowCount: " + rowCount)

      for (j <- 0 until rowCount) {
        println(s"row: $j")
        imported match {
          case vector: IntVector =>
            val value = vector.get(j)
            println(s"value: $value")
          case vector: BigIntVector =>
            val value = vector.get(j)
            println(s"value: $value")
        }
      }
      arrowArray.close()
      arrowSchema.close()
    }
  }
}