package core

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DateType

object schemas {
  val ymmtSchema = StructType(
    StructField("vin_prefix", StringType, false) ::
    StructField("year", StringType, false) ::
    StructField("make", StringType, false) ::
    StructField("model", StringType, false) ::
    StructField("trim", StringType, false) ::
    StructField("ymmt_id", StringType, false) :: Nil
  )
  
  val zipcodeSchema = StructType(
    StructField("zipcode", StringType, false) ::
    StructField("state", StringType, false) :: Nil
  )
  
  val rawSaleRecordSchema = StructType(
    StructField("vin", StringType, false) ::
    StructField("date", DateType, false) ::
    StructField("price", LongType, false) ::
    StructField("miles", LongType, true) ::
    StructField("zip", IntegerType, true) :: Nil
  )
  
  val saleRecordSchema = StructType(
    StructField("vin", StringType, false) ::
    StructField("date", DateType, false) ::
    StructField("price", LongType, false) ::
    StructField("miles", LongType, true) ::
    StructField("zip", IntegerType, true) ::
    StructField("ymmt_id", StringType, false) :: Nil
  )
}