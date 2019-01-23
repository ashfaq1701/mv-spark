package processes

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import core.session
import org.apache.spark.sql.DataFrame
import models.SaleRecord
import udfs.Udfs
import core.globals

object ModZScoreOutlierDetection {
  def rejectOutlier(dataset: Dataset[SaleRecord], column: String, columns: String*) : DataFrame = {
    import session.spark.implicits._
    val absDeviation = udf(Udfs.absDeviation)
    val modZScore = udf(Udfs.modZScore)
    val modZScoreOutlier = udf(Udfs.isModZScoreOutlier)
    dataset.createOrReplaceTempView("passed")
    val df1 = session.spark.sql("select " + globals.columns(column, columns:_*) + ", percentile_approx(price, 0.5) as median_y from passed group by " + globals.columns(column, columns:_*) + " order by " + globals.columns(column, columns:_*))
    df1.createOrReplaceTempView("intermediate1")
    val joinedDf1 = session.spark.sql("select passed.*, intermediate1.median_y as median_y from passed left outer join intermediate1 on " + globals.colMap("intermediate1", "passed", column, columns:_*))
    val df2 = joinedDf1.withColumn("abs_deviation_y", absDeviation($"price", $"median_y"))
    df2.createOrReplaceTempView("intermediate2")
    val df3 = session.spark.sql("select " + globals.columns(column, columns:_*) + ", percentile_approx(abs_deviation_y, 0.5) as median_abs_deviation_y from intermediate2 group by " + globals.columns(column, columns:_*) + " order by " + globals.columns(column, columns:_*))
    df3.createOrReplaceTempView("intermediate3")
    val joinedDf2 = session.spark.sql("select intermediate2.*, intermediate3.median_abs_deviation_y as median_abs_deviation_y from intermediate2 left outer join intermediate3 on " + globals.colMap("intermediate3", "intermediate2", column, columns:_*))
    val df4 = joinedDf2.withColumn("modified_z_scores", modZScore($"price", $"median_y", $"median_abs_deviation_y"))
    val df5 = df4.filter(modZScoreOutlier($"modified_z_scores"))
    df5
  }
}