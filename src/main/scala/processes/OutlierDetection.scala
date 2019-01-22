package processes

import models.SaleRecord
import core.session
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import udfs.Regression
import udfs.MseNormxsq

object OutlierDetection {
  def rejectOutlier(dataset: Dataset[SaleRecord], column: String) : DataFrame = {
    import session.spark.implicits._
    val df1 = dataset.groupBy(column)
    .agg(count(col("*")).as("total_records"), mean(col("miles")).as("mean_miles"),
      mean(col("price")).as("mean_price"),
      Regression(col("miles"), col("price"))("slope").as("regression_slope"),
      Regression(col("miles"), col("price"))("intercept").as("regression_intercept")
    )
    
    df1.createOrReplaceTempView("passed")
    df1.createOrReplaceTempView("intermediate1")
    
    val joinedDf1 = session.spark.sql(s"SELECT passed*, intermediate1.total_records as total_records, intermediate1.mean_miles as mean_miles, intermediate1.mean_price as mean_price, intermediate1.regression_slope as regression_slope, intermediate1.regression_intercept as regression_intercept from passed LEFT OUTER JOIN intermediate1 ON intermediate1.$column = passed.$column")
    joinedDf1.createOrReplaceTempView("intermediate2")
    
    val df2 = joinedDf1.groupBy(column)
    .agg(
      MseNormxsq(col("miles"), col("price"), col("mean_miles"), col("regression_slope"), col("regression_intercept"))("normxsq").as("normxsq"),
      MseNormxsq(col("miles"), col("price"), col("mean_miles"), col("regression_slope"), col("regression_intercept"))("mse").as("mse")
    )
    
    df2.createOrReplaceTempView("intermediate3")
    
    val joinedDf2 = session.spark.sql(s"SELECT intermediate2.*, intermediate3.normxsq as normxsq, intermediate3.mse as mse from intermediate2 LEFT OUTER JOIN intermediate3 ON intermediate3.$column = intermediate2.$column")
    joinedDf2.createOrReplaceTempView("intermediate4")
    
    joinedDf2
  }
}