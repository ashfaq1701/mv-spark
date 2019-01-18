package core

import org.apache.spark.sql.SparkSession

object session {
  val spark = SparkSession.builder()
    .appName("Application to summarize sale records")
    .master("local")
    .enableHiveSupport()
    .getOrCreate()
}