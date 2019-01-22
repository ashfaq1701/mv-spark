package udfs

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object MseNormxsq extends UserDefinedAggregateFunction {
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(
      StructField("x", LongType) ::
      StructField("y", LongType) ::
      StructField("mean_x", DoubleType) :: 
      StructField("regression_slope", DoubleType) :: 
      StructField("regression_intercept", DoubleType) :: Nil
    )

  override def bufferSchema: StructType = StructType(
    StructField("count", LongType) ::
    StructField("normxsq", DoubleType) :: 
    StructField("sqres", DoubleType) :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = StructType(
    StructField("normxsq", DoubleType) :: 
    StructField("mse", DoubleType) :: Nil
  )

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0.0
    buffer(2) = 0.0
  }
  
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Long](0) + 1
    buffer(1) = buffer.getAs[Double](1) + math.pow((input.getAs[Long](0).toDouble - input.getAs[Double](2)), 2)
    val pyi = input.getAs[Double](4) + (input.getAs[Double](3) * input.getAs[Long](0).toDouble)
    buffer(2) = buffer.getAs[Double](2) + math.pow((input.getAs[Long](1).toDouble - pyi), 2)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    buffer1(1) = buffer1.getAs[Double](1) + buffer2.getAs[Double](1)
    buffer1(2) = buffer1.getAs[Double](2) + buffer2.getAs[Double](2)
  }

  override def evaluate(buffer: Row): Any = {
    val mse = buffer.getAs[Double](2) / buffer.getAs[Long](0).toDouble
    (buffer.getAs[Double](1), mse)
  }
}