package core

import org.apache.spark.sql.SparkSession

object session {
  val spark = SparkSession.builder()
    .appName("Application to summarize sale records")
    .master("local")
    .config("spark.sql.warehouse.dir", "/var/hive-warehouse")
    .enableHiveSupport()
    .getOrCreate()
}