package processes

import models.SaleRecord
import core.session
import core.globals
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import udfs.Regression
import udfs.MseNormxsq
import udfs.Udfs

object CookDOutlierDetection {
  def rejectOutlier(dataset: Dataset[SaleRecord], column: String, columns: String*) : DataFrame = {
    import session.spark.implicits._
    val cookDistance = udf(Udfs.cooksDistance)
    val isOutlier = udf(Udfs.isOutlier)
    val df1 = dataset.groupBy(column, columns:_*)
    .agg(count(col("*")).as("total_records"), mean(col("miles")).as("mean_miles"),
      mean(col("price")).as("mean_price"),
      Regression(col("miles"), col("price"))("slope").as("regression_slope"),
      Regression(col("miles"), col("price"))("intercept").as("regression_intercept")
    )
    
    dataset.createOrReplaceTempView("passed")
    df1.createOrReplaceTempView("intermediate1")
    
    val joinedDf1 = session.spark.sql("SELECT passed.*, intermediate1.total_records as total_records, intermediate1.mean_miles as mean_miles, intermediate1.mean_price as mean_price, intermediate1.regression_slope as regression_slope, intermediate1.regression_intercept as regression_intercept from passed LEFT OUTER JOIN intermediate1 ON " + globals.colMap("intermediate1", "passed", column, columns:_*))
    joinedDf1.createOrReplaceTempView("intermediate2")
    
    val df2 = joinedDf1.groupBy(column, columns:_*)
    .agg(
      MseNormxsq(col("miles"), col("price"), col("mean_miles"), col("regression_slope"), col("regression_intercept"))("normxsq").as("normxsq"),
      MseNormxsq(col("miles"), col("price"), col("mean_miles"), col("regression_slope"), col("regression_intercept"))("mse").as("mse")
    )
    
    df2.createOrReplaceTempView("intermediate3")
    
    val joinedDf2 = session.spark.sql("SELECT intermediate2.*, intermediate3.normxsq as normxsq, intermediate3.mse as mse from intermediate2 LEFT OUTER JOIN intermediate3 ON " + globals.colMap("intermediate3", "intermediate2", column, columns:_*))
    
    val joinedDf3 = joinedDf2.withColumn("cook_distance", cookDistance($"miles", $"price", $"total_records", $"mean_miles", $"regression_slope", $"regression_intercept", $"mse", $"normxsq"))
    joinedDf3.createOrReplaceTempView("intermediate4")
    
    val df3 = joinedDf3.groupBy(column, columns:_*)
    .agg(mean(col("cook_distance")).as("cook_distance_mean"))
    df3.createOrReplaceTempView("intermediate5")
    
    val joinedDf4 = session.spark.sql("SELECT intermediate4.*, intermediate5.cook_distance_mean as cook_distance_mean FROM intermediate4 LEFT OUTER JOIN intermediate5 ON " + globals.colMap("intermediate5", "intermediate4", column, columns:_*))
    val df5 = joinedDf4.withColumn("is_outlier", isOutlier($"cook_distance", $"cook_distance_mean"))
    
    val result = df5.filter($"is_outlier" === 0)
    
    session.spark.catalog.dropTempView("passed")
    session.spark.catalog.dropTempView("intermediate1")
    session.spark.catalog.dropTempView("intermediate2")
    session.spark.catalog.dropTempView("intermediate3")
    session.spark.catalog.dropTempView("intermediate4")
    session.spark.catalog.dropTempView("intermediate5")
    
    result
  }
}