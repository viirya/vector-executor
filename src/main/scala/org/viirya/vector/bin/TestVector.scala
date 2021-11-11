package org.viirya.vector.bin

import java.io.ByteArrayOutputStream

import scala.collection.JavaConverters._

import org.apache.arrow.ffi.FFI
import org.apache.arrow.ffi.ArrowArray
import org.apache.arrow.ffi.ArrowSchema
import org.apache.arrow.ffi.FFIDictionaryProvider
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BigIntVector
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.IntVector

import org.apache.spark.sql.catalyst.expressions.{Add, Alias, Attribute, AttributeReference, BindReferences, BoundReference, Expression}
import org.apache.spark.sql.execution.{LocalTableScanExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.serde._
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.serde.ExprOuterClass.Expr
import org.apache.spark.sql.execution.serde.OperatorOuterClass.Operator

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

    testQueryPlanSerialization(lib)
  }

  def exprToProto(expr: Expression, inputs: Seq[Attribute]): Expr = {
    expr match {
      case Alias(child, name) =>
        exprToProto(child, inputs)

      case Add(left, right, failOnError) =>
        val addBuilder = ExprOuterClass.Add.newBuilder()
        addBuilder.setLeft(exprToProto(left, inputs))
        addBuilder.setRight(exprToProto(right, inputs))

        ExprOuterClass.Expr.newBuilder()
          .setAdd(addBuilder).build()

      case attr: AttributeReference =>
        val boundRef= BindReferences.bindReference(attr, inputs, allowFailures = false)
          .asInstanceOf[BoundReference]
        ExprOuterClass.Expr.newBuilder()
          .setBound(ExprOuterClass.BoundReference.newBuilder().setIndex(boundRef.ordinal).build())
          .build()
    }
  }

  def operatorToProto(op: SparkPlan): Option[Operator] = {
    op match {
      case ProjectExec(projectList, child) =>
        val exprs = projectList.map(exprToProto(_, child.output))
        val projectBuilder = OperatorOuterClass.Projection.newBuilder()
        projectBuilder.addAllProjectList(exprs.toIterable.asJava)

        val opBuilder = OperatorOuterClass.Operator.newBuilder()
        Some(opBuilder.setProjection(projectBuilder).build())

      case _ => None
    }
  }

  def testQueryPlanSerialization(lib: VectorLib): Unit = {
    // Build proto objects.
    // Add(bound(0), bound(1))
    val outputs = Seq(AttributeReference("a", IntegerType)(), AttributeReference("b", IntegerType)())
    val projectExec = ProjectExec(
      Seq(Alias(Add(outputs(0), outputs(1)), "add")()),
      LocalTableScanExec(outputs, Seq.empty))
    val op = operatorToProto(projectExec).get

    val outputStream = new ByteArrayOutputStream()
    op.writeTo(outputStream)
    outputStream.close()

    // serialized query plan
    val bytes = outputStream.toByteArray

    // input vectors
    val schema = Seq(StructField("a", IntegerType))
    val vector = OffHeapColumnVector.allocateColumns(10, schema.toArray)(0)
    vector.putInt(0, 456)
    val vectorAddress = vector.valuesNativeAddress()
    val anotherVector = OffHeapColumnVector.allocateColumns(10, schema.toArray)(0)
    anotherVector.putInt(0, 123)
    val anotherVectorAddress = anotherVector.valuesNativeAddress()

    println(s"Executing query plan: ${op.toString}")
    val plan = lib.createPlan(bytes)
    for (i <- 0 to 10) {
      println(s"i = $i")
      val array_addresses = lib.executePlan(plan, Array(vectorAddress, anotherVectorAddress), 10)
      read_arrow_arrays(array_addresses)
    }
    lib.releasePlan(plan)
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