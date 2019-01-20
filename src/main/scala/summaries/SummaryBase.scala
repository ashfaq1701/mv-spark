package summaries

import org.apache.spark.sql.Dataset
import models.SaleRecord
import java.sql.Date

object SummaryBase {
  def computeSummaryBase(dataset: Dataset[SaleRecord], maxSaleDate: Date) : Unit = {
    
  }
}