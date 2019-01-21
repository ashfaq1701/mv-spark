package summaries

import org.apache.spark.sql.Dataset
import models.SaleRecord
import java.sql.Date

object SummaryByState {
  def computeSummaryByState(dataset: Dataset[SaleRecord], maxSaleDate: Date) : Unit = {
    dataset.groupBy("ymmt_id", "state")
  }
}