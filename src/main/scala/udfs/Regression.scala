package udfs

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object Regression extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType = StructType(
    StructField("col1", LongType) :: 
    StructField("col2", LongType) :: Nil
  )

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("count", LongType) ::
    StructField("sxy", DoubleType) :: 
    StructField("sx", DoubleType) :: 
    StructField("sy", DoubleType) :: 
    StructField("sxsq", DoubleType) :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = StructType(
    StructField("slope", DoubleType) ::
    StructField("intercept", DoubleType) :: Nil
  )

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0.0
    buffer(2) = 0.0
    buffer(3) = 0.0
    buffer(4) = 0.0
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Long](0) + 1L
    buffer(1) = buffer.getAs[Double](1) + (input.getAs[Long](0).toDouble * input.getAs[Long](1).toDouble)
    buffer(2) = buffer.getAs[Double](2) + input.getAs[Long](0).toDouble
    buffer(3) = buffer.getAs[Double](3) + input.getAs[Long](1).toDouble
    buffer(4) = buffer.getAs[Double](3) + math.pow(input.getAs[Long](0).toDouble, 2)
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    buffer1(1) = buffer1.getAs[Double](1) + buffer2.getAs[Double](1)
    buffer1(2) = buffer1.getAs[Double](2) + buffer2.getAs[Double](2)
    buffer1(3) = buffer1.getAs[Double](3) + buffer2.getAs[Double](3)
    buffer1(4) = buffer1.getAs[Double](4) + buffer2.getAs[Double](4)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    val denom = (buffer.getLong(0).toDouble * buffer.getDouble(4)) - math.pow(buffer.getDouble(2), 2)
    var m = 0.0
    if (denom != 0.0) {
      m = ((buffer.getLong(0).toDouble * buffer.getDouble(1)) - (buffer.getDouble(2) * buffer.getDouble(3))) / denom
    }
    var b = 0.0
    if (buffer.getLong(0) != 0L) {
      b = (buffer.getDouble(3) - m * buffer.getDouble(2)) / buffer.getLong(0).toDouble
    }
    (m, b)
  }
}