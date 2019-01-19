package salerecords

import core.database
import core.session
import core.schemas
import models.YmmtId

object ImportSaleRecords {
  def importRecords (path : String): Unit = {
    database.createDatabase
    import session.spark.implicits._
    if (!session.spark.catalog.tableExists("salerecords", "ymmt_ids")) {
      val ymmtDSToStore = session.spark.read
      .option("inferSchema", "true")
      .schema(schemas.ymmtSchema)
      .csv(path + "/ymmt_ids.csv")
      .as[YmmtId]
      ymmtDSToStore.write.saveAsTable("salerecords.ymmt_ids")
    }
    val ymmtDS = session.spark.read
    .table("salerecords.ymmt_ids")
    .as[YmmtId]
  }
}