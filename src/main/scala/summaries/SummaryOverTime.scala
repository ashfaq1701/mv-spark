package summaries

import org.apache.spark.sql.Dataset
import models.SaleRecord
import java.sql.Date

object SummaryOverTime {
  def computeSummaryOverTime(dataset: Dataset[SaleRecord], maxSaleDate: Date) : Unit = {
    
  }
}