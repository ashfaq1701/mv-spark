package core

import java.text.SimpleDateFormat
import java.util.Calendar
import java.io.File

object database {
  def createDatabase : Unit = {
    if (!session.spark.catalog.databaseExists("salerecords")) {
      session.spark.sql("CREATE DATABASE salerecords")
    }
  }
  
  def deleteDatabase : Unit = {
    session.spark.sql("DROP DATABASE IF EXISTS salerecords CASCADE")
    val warehouse = session.spark.conf.get("spark.sql.warehouse.dir").replace("file:", "")
    globals.deleteFolder(new File(warehouse + "salerecords.db"))
  }
  
  def deleteOlderPartitions : Unit = {
    var dateBeforeSixMonths = Calendar.getInstance()
    dateBeforeSixMonths.add(Calendar.MONTH, -1)
    dateBeforeSixMonths.set(Calendar.DAY_OF_MONTH, dateBeforeSixMonths.getActualMaximum(Calendar.DAY_OF_MONTH))
    dateBeforeSixMonths.add(Calendar.MONTH, -6)
    val partitions = session.spark.sql("SHOW PARTITIONS salerecords.salerecords")
    val partitionsArr = partitions.collect
    partitionsArr.foreach(partition => {
      val partitionName = partition.getAs[String](0)
      println(s"Found partition $partitionName.")
      val partitionSplit = partitionName.split("=")
      if (partitionSplit.length > 1) {
        val col = partitionSplit(0)
        val ym = partitionSplit(1)
        val format = new SimpleDateFormat("yyyy-M")
        val date = format.parse(ym)
        if (date.before(dateBeforeSixMonths.getTime)) {
          println(s"$partitionName is older than 6 months, deleting partition $partitionName.")
          session.spark.sql(s"ALTER TABLE salerecords.salerecords DROP IF EXISTS PARTITION($col='$ym')")
        }
      }
    })
  }
}