package main

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import core.session

object SummarySaleRecords {
  def main(args: Array[String]): Unit = {
    val spark = session.spark
    val df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/home/ashfaq/Downloads/FL_insurance_sample/FL_insurance_sample.csv")
  }
}