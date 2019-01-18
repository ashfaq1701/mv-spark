package main

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object SummarySaleRecords {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
    .appName("Application to summarize sale records")
    .master("local")
    .enableHiveSupport()
    .getOrCreate()
    
    val df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/home/ashfaq/Downloads/FL_insurance_sample/FL_insurance_sample.csv")
    
    df.write.saveAsTable("testdb.testdata")
  }
}