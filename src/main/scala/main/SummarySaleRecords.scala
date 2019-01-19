package main

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import salerecords.ImportSaleRecords

object SummarySaleRecords {
  def main(args: Array[String]): Unit = {
    ImportSaleRecords.importRecords("/usr/dump_data");
  }
}