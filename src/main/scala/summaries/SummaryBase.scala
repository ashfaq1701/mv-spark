package summaries

import org.apache.spark.sql.Dataset
import models.SaleRecord
import java.sql.Date
import org.apache.spark.sql.functions._
import core.session
import core.globals
import udfs.Certainty
import udfs.Regression
import processes.CookDOutlierDetection
import processes.ZScoreOutlierDetection
import processes.ModZScoreOutlierDetection
import org.apache.spark.sql.Column
import models.SummaryBase

object SummaryBase {
  def computeSummaryBase(dataset: Dataset[SaleRecord], maxSaleDate: Date, outlierDetection: String = "z_score") : Unit = {
    import session.spark.implicits._
    
    val threeMonthWindow = dataset.filter(col("date") > globals.minusMonths(maxSaleDate, 3))
    val sixMonthWindow = dataset.filter(col("date") > globals.minusMonths(maxSaleDate, 6))
    
    var filteredThreeMonthDF = session.spark.emptyDataFrame
    var filteredSixMonthDF = session.spark.emptyDataFrame
    
    if (outlierDetection == "cooks_distance") {
      filteredThreeMonthDF = CookDOutlierDetection.rejectOutlier(threeMonthWindow, "ymmt_id")
      filteredSixMonthDF = CookDOutlierDetection.rejectOutlier(sixMonthWindow, "ymmt_id")
    } else if (outlierDetection == "mod_z_score") {
      filteredThreeMonthDF = ModZScoreOutlierDetection.rejectOutlier(threeMonthWindow, "ymmt_id")
      filteredSixMonthDF = ModZScoreOutlierDetection.rejectOutlier(sixMonthWindow, "ymmt_id")
    } else {
      filteredThreeMonthDF = ZScoreOutlierDetection.rejectOutlier(threeMonthWindow, "ymmt_id")
      filteredSixMonthDF = ZScoreOutlierDetection.rejectOutlier(sixMonthWindow, "ymmt_id")
    }
    
    val threeMonthSummary = filteredThreeMonthDF.groupBy("ymmt_id")
    .agg(min(col("date")).as("start_date"), 
        max(col("date")).as("end_date"), count(col("*")).as("total_records"), 
        mean(col("price")).as("mean"), stddev(col("price")).as("stdev"),
        Certainty(col("price")).as("certainty"), sum(col("price")).as("price_total"), 
        (mean(col("price")).plus(stddev(col("price")).multiply(1.5))).as("price_above"),
        (mean(col("price")).minus(stddev(col("price")).multiply(1.5))).as("price_below"),
        Regression(col("miles"), col("price"))("slope").as("regression_slope"),
        Regression(col("miles"), col("price"))("intercept").as("regression_intercept"),
        (max(col("price")) - min(col("price"))).as("price_depriciation_total"),
        (max(col("miles")) - min(col("miles"))).as("miles_depriciation"),
        sum(col("miles")).as("total_miles")).withColumn("month_window", lit(3))
        .as[SummaryBase]
    val filteredThreeMonths = threeMonthSummary.filter(col("stdev").isNaN === false)
    filteredThreeMonths.show
        
    val sixMonthSummary = filteredSixMonthDF.groupBy("ymmt_id")
    .agg(min(col("date")).as("start_date"), 
        max(col("date")).as("end_date"), count(col("*")).as("total_records"), 
        mean(col("price")).as("mean"), stddev(col("price")).as("stdev"),
        Certainty(col("price")).as("certainty"), sum(col("price")).as("price_total"), 
        (mean(col("price")).plus(stddev(col("price")).multiply(1.5))).as("price_above"),
        (mean(col("price")).minus(stddev(col("price")).multiply(1.5))).as("price_below"),
        Regression(col("miles"), col("price"))("slope").as("regression_slope"),
        Regression(col("miles"), col("price"))("intercept").as("regression_intercept"),
        (max(col("price")) - min(col("price"))).as("price_depriciation_total"),
        (max(col("miles")) - min(col("miles"))).as("miles_depriciation"),
        sum(col("miles")).as("total_miles")).withColumn("month_window", lit(6))
        .as[SummaryBase]
    val filteredSixMonths = sixMonthSummary.filter(col("stdev").isNaN === false)
    filteredSixMonths.show
  }
}