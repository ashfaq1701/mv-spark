package salerecords

import core.database
import core.session
import core.schemas
import models.YmmtId
import models.RawSaleRecord
import models.RawSaleRecordWithVinPrefix
import models.SaleRecord
import java.io.File
import org.apache.spark.sql.functions._

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
    val ymmtDS = session.spark.read
    .table("salerecords.ymmt_ids")
    .as[YmmtId]
    
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
    
    val joinedDS = session.spark.sql("SELECT tmp_salerecords.*, tmp_ymmt_ids.ymmt_id FROM tmp_salerecords INNER JOIN tmp_ymmt_ids ON tmp_salerecords.vin_prefix = tmp_ymmt_ids.vin_prefix")
    .as[SaleRecord]
    //joinedDS.write.mode("append").saveAsTable("salerecords.salerecords")
    println(joinedDS.count())
  }
  
  def summarizeRecords : Unit = {
    if (!session.spark.catalog.tableExists("salerecords", "salerecords")) {
      println("Salerecords table does not exist")
      sys.exit(1)
    }
  }
  
  def deleteTable : Unit = {
    if (session.spark.catalog.tableExists("salerecords", "salerecords")) {
      session.spark.sql("DROP TABLE salerecords")
      println("Salerecords table dropped")
    } else {
      println("Salerecords table does not exist")
    }
  }
}