package processes

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import core.session
import org.apache.spark.sql.DataFrame
import models.SaleRecord
import udfs.Udfs
import core.globals

object ZScoreOutlierDetection {
  def rejectOutlier(dataset: Dataset[SaleRecord], column: String, columns: String*) : DataFrame = {
    import session.spark.implicits._
    val zScore = udf(Udfs.zScore)
    val df1 = dataset.groupBy(column, columns:_*)
    .agg(mean(col("price")).as("price_mean"), stddev(col("price")).as("price_stddev"))
    dataset.createOrReplaceTempView("passed")
    df1.createOrReplaceTempView("intermediate1")
    val joinedDf1 = core.session.spark.sql("SELECT passed.*, intermediate1.price_mean as price_mean, intermediate1.price_stddev as price_stddev FROM passed LEFT OUTER JOIN intermediate1 ON " + globals.colMap("passed", "intermediate1", column, columns:_*))
    val df2 = joinedDf1.withColumn("z_index", zScore($"price", $"price_mean", $"price_stddev"))
    val df3 = df2.filter($"z_index" <= 3)
    
    session.spark.catalog.dropTempView("passed")
    session.spark.catalog.dropTempView("intermediate1")
    df3
  }
}