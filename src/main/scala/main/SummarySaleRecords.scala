package main

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import salerecords.SaleRecords
import core.session
import core.database
import core.globals

object SummarySaleRecords {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("You must pass the action to run this application")
      sys.exit(1)
    }
    val cmd = args(0)
    if (cmd == "import") {
       SaleRecords.importRecords(System.getProperty("user.home") + "/dump_data");
    } else if (cmd == "summary") {
      if (args.length == 2) {
        SaleRecords.summarizeRecords(args(1))
      } else {
        SaleRecords.summarizeRecords()
      }
    } else if (cmd == "delete") {
      SaleRecords.deleteTable
    } else if (cmd == "delete_database") {
      database.deleteDatabase
    } else if (cmd == "show") {
      SaleRecords.showSaleRecordsCount
    } else if (cmd == "delete_partitions") {
      database.deleteOlderPartitions
    } else {
      println("Wrong action")
      sys.exit(1)
    }
  }
}