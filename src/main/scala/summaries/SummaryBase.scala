package summaries

import org.apache.spark.sql.Dataset
import models.SaleRecord
import java.sql.Date
import org.apache.spark.sql.functions._
import core.session
import core.globals
import udfs.Certainty
import org.apache.spark.sql.Column

object SummaryBase {
  def computeSummaryBase(dataset: Dataset[SaleRecord], maxSaleDate: Date) : Unit = {
    import session.spark.implicits._
    
    val threeMonthWindow = dataset.filter(col("date") > globals.minusMonths(maxSaleDate, 3))
    .groupBy("ymmt_id")
    val sixMonthWindow = dataset.filter(col("date") > globals.minusMonths(maxSaleDate, 6))
    .groupBy("ymmt_id")
    
    val threeMonthSummary = threeMonthWindow
    .agg(first(col("ymmt_id")).as("ymmt_id"), min(col("date")).as("start_date"), 
        max(col("date")).as("end_date"), count(col("*")).as("total_records"), 
        mean(col("price")).as("mean"), stddev(col("price")).as("stdev"), 
        Certainty(col("price")).as("certainty"), sum(col("price")).as("price_total"), 
        (mean(col("price")).plus(stddev(col("price")).multiply(1.5))).as("price_above"),
        (mean(col("price")).minus(stddev(col("price")).multiply(1.5))).as("price_below"),
        sum(col("miles")).as("total_mileage"))
    threeMonthSummary.show
  }
}