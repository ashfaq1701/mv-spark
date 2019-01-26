package summaries

import org.apache.spark.sql.Dataset
import models.SaleRecord
import java.sql.Date
import org.apache.spark.sql.functions._
import core.session
import processes.CookDOutlierDetection
import processes.ZScoreOutlierDetection
import processes.ModZScoreOutlierDetection

object SummaryOverTime {
  def computeSummaryOverTime(dataset: Dataset[SaleRecord], outlierDetection: String = "z_score") : Unit = {
    import session.spark.implicits._
    var filteredDF = session.spark.emptyDataFrame
    if (outlierDetection == "cooks_distance") {
      filteredDF = CookDOutlierDetection.rejectOutlier(dataset, "ymmt_id", "year_month")
    } else if (outlierDetection == "mod_z_score") {
      filteredDF = ModZScoreOutlierDetection.rejectOutlier(dataset, "ymmt_id", "year_month")
    } else {
      filteredDF = ZScoreOutlierDetection.rejectOutlier(dataset, "ymmt_id", "year_month")
    }
    val summary = filteredDF.groupBy("ymmt_id", "year_month")
    .agg(count(col("*")).as("total_records"), sum(col("price")).as("price_total"))
    .as[models.SummaryOverTime]
    summary.show
  }
}