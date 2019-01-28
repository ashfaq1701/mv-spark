package salerecords

import core.database
import core.session
import core.schemas
import core.globals
import models.YmmtId
import models.Zipcode
import models.RawSaleRecord
import models.RawSaleRecordWithVinPrefix
import models.SaleRecord
import java.io.File
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import java.util.Calendar
import java.sql.Date
import java.text.SimpleDateFormat
import java.text.DateFormat
import summaries.SummaryBase
import summaries.SummaryByState
import summaries.SummaryOverTime

object SaleRecords {
  def importRecords (path : String): Unit = {
    database.createDatabase
    import session.spark.implicits._
    if (!session.spark.catalog.tableExists("salerecords", "ymmt_ids")) {
      val ymmtDSToStore = session.spark.read
      .schema(schemas.ymmtSchema)
      .csv(path + "/ymmt_ids.csv")
      .as[YmmtId]
      ymmtDSToStore.write.saveAsTable("salerecords.ymmt_ids")
    }
    if (!session.spark.catalog.tableExists("salerecords", "zipcodes")) {
      val zipcodesDSToStore = session.spark.read
      .option("header", "true")
      .schema(schemas.zipcodeSchema)
      .csv(path + "/zipcodes.csv")
      .as[Zipcode]
      zipcodesDSToStore.write.saveAsTable("salerecords.zipcodes")
    }
    val ymmtDS = session.spark.read
    .table("salerecords.ymmt_ids")
    .as[YmmtId]
    
    val zipcodesDS = session.spark.read
    .table("salerecords.zipcodes")
    .as[Zipcode]
    
    var mergedDS = session.spark.emptyDataset[RawSaleRecord]
    
    val saleRecordFiles = new File(path + "/salerecords").listFiles
    for (saleRecordFile <- saleRecordFiles) {
      val saleRecordDS = session.spark.read
      .option("header", "true")
      .schema(schemas.rawSaleRecordSchema)
      .csv(saleRecordFile.toString())
      .as[RawSaleRecord]
      mergedDS = mergedDS.union(saleRecordDS)
    }
    
    val mergedDSWithVinPrefix = mergedDS.select(col("*"),
        concat(substring(col("vin"), 0, 8), substring(col("vin"), 10, 2)).as("vin_prefix"))
        .as[RawSaleRecordWithVinPrefix]
    
    mergedDSWithVinPrefix.createOrReplaceTempView("tmp_salerecords")
    ymmtDS.createOrReplaceTempView("tmp_ymmt_ids")
    zipcodesDS.createOrReplaceTempView("tmp_zipcodes")
    
    val joinedDS = session.spark.sql("SELECT tmp_salerecords.*, trim(tmp_ymmt_ids.ymmt_id) as ymmt_id, tmp_zipcodes.state, concat(year(tmp_salerecords.date), '-', month(tmp_salerecords.date)) AS year_month FROM tmp_salerecords INNER JOIN tmp_ymmt_ids ON tmp_salerecords.vin_prefix = tmp_ymmt_ids.vin_prefix LEFT OUTER JOIN tmp_zipcodes ON tmp_salerecords.zip = tmp_zipcodes.zipcode")
    .as[SaleRecord]
    joinedDS.write.partitionBy("year_month")
    .mode("append")
    .saveAsTable("salerecords.salerecords")
  }
  
  def summarizeRecords(outlierDetection: String = "z_score") : Unit = {
    if (!session.spark.catalog.tableExists("salerecords", "salerecords")) {
      println("Salerecords table does not exist")
      sys.exit(1)
    }
    import session.spark.implicits._
    val saleRecordsDS = session.spark.read
    .table("salerecords.salerecords")
    .as[SaleRecord]
    saleRecordsDS.createOrReplaceTempView("tmp_salerecords")
    val maxSaleDate = session.spark.sql("SELECT max(date) from tmp_salerecords").first.getDate(0)
    val uniqueDS = saleRecordsDS.dropDuplicates("vin", "date")
    val filteredDS = uniqueDS.filter(col("price") > 0 && col("miles")> 50 && col("miles") < 3000000)
    session.spark.catalog.dropTempView("tmp_salerecords")
    SummaryBase.computeSummaryBase(filteredDS, maxSaleDate, outlierDetection)
    SummaryByState.computeSummaryByState(filteredDS, maxSaleDate, outlierDetection)
    SummaryOverTime.computeSummaryOverTime(filteredDS, outlierDetection)
  }
  
  def showSaleRecordsCount: Unit = {
    if (!session.spark.catalog.tableExists("salerecords", "salerecords")) {
      println("Salerecords table does not exist")
      sys.exit(1)
    }
    import session.spark.implicits._
    val saleRecordsDS = session.spark.read
    .table("salerecords.salerecords")
    .as[SaleRecord]
    println(saleRecordsDS.count)
    saleRecordsDS.show
  }
  
  def deleteTable() : Unit = {
    if (session.spark.catalog.tableExists("salerecords", "salerecords")) {
      session.spark.sql("DROP TABLE salerecords.salerecords")
      println("Salerecords table dropped")
    } else {
      println("Salerecords table does not exist")
    }
    val warehouse = session.spark.conf.get("spark.sql.warehouse.dir").replace("file:", "")
    globals.deleteFolder(new File(warehouse + "/salerecords.db/salerecords"))
  }
}