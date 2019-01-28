package export

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import core.conf
import models.SummaryBase
import models.SummaryByState
import models.SummaryOverTime
import java.util.Properties
import core.session
import java.sql.SQLSyntaxErrorException
import org.apache.spark.sql.SaveMode

object MysqlExport {
  val mysqlConf = conf.parseMysqlConf
  Class.forName("com.mysql.cj.jdbc.Driver")
  
  def getJdbcUrl () : String = {
    s"jdbc:mysql://${this.mysqlConf("host")}:${this.mysqlConf("port")}/${this.mysqlConf("database")}"
  }
  
  def getConnectionProperties () : Properties = {
    val connectionProperties = new Properties
    connectionProperties.put("user", s"${this.mysqlConf("user")}")
    connectionProperties.put("password", s"${this.mysqlConf("password")}")
    connectionProperties
  }
  
  def exportSummaryBase(dataset: Dataset[SummaryBase]) : Unit = {
    import session.spark.implicits._
    var ds = session.spark.emptyDataset[SummaryBase]
    try {
      ds = session.spark.read.jdbc(this.getJdbcUrl, "summary_base", this.getConnectionProperties)
      .select("ymmt_id", "month_window", "start_date", "end_date", "total_records", "mean", "stdev", "certainty", "price_total",
          "price_above", "price_below", "regression_slope", "regression_intercept", "price_depriciation_total", "miles_depriciation", 
          "total_miles")
      .as[SummaryBase]
    } catch {
      case sqlSyntaxErr: SQLSyntaxErrorException => {
        ds = session.spark.emptyDataset[SummaryBase]
      }
    }
    val joined = ds.join(right = dataset, usingColumns = Seq("ymmt_id", "month_window"), joinType = "full").select(
      col("ymmt_id"), col("month_window"),
      coalesce(dataset("start_date"), ds("start_date")).as("start_date"),
      coalesce(dataset("end_date"), ds("end_date")).as("end_date"),
      coalesce(dataset("total_records"), ds("total_records")).as("total_records"),
      coalesce(dataset("mean"), ds("mean")).as("mean"),
      coalesce(dataset("stdev"), ds("stdev")).as("stdev"),
      coalesce(dataset("certainty"), ds("certainty")).as("certainty"),
      coalesce(dataset("price_total"), ds("price_total")).as("price_total"),
      coalesce(dataset("price_above"), ds("price_above")).as("price_above"),
      coalesce(dataset("price_below"), ds("price_below")).as("price_below"),
      coalesce(dataset("regression_slope"), ds("regression_slope")).as("regression_slope"),
      coalesce(dataset("regression_intercept"), ds("regression_intercept")).as("regression_intercept"),
      coalesce(dataset("price_depriciation_total"), ds("price_depriciation_total")).as("price_depriciation_total"),
      coalesce(dataset("miles_depriciation"), ds("miles_depriciation")).as("miles_depriciation"),
      coalesce(dataset("total_miles"), ds("total_miles")).as("total_miles")
    )
    joined.withColumn("id", monotonically_increasing_id()).write
     .mode(SaveMode.Overwrite)
     .jdbc(this.getJdbcUrl, "summary_base_new", this.getConnectionProperties)
  }
  
  def exportSummaryByState(dataset: Dataset[SummaryByState]) : Unit = {
    import session.spark.implicits._
    var ds = session.spark.emptyDataset[SummaryByState]
    try {
      ds = session.spark.read.jdbc(this.getJdbcUrl, "summary_by_state", this.getConnectionProperties)
      .select("ymmt_id", "state", "month_window", "start_date", "end_date", "total_records", "mean", "stdev", "certainty",
          "price_total", "price_above", "price_below", "regression_slope", "regression_intercept", "price_depriciation_total", 
          "miles_depriciation", "total_miles")
      .as[SummaryByState]
    } catch {
      case sqlSyntaxErr: SQLSyntaxErrorException => {
        ds = session.spark.emptyDataset[SummaryByState]
      }
    }
    val joined = ds.join(right = dataset, usingColumns = Seq("ymmt_id", "state", "month_window"), joinType = "full").select(
      col("ymmt_id"), col("state"), col("month_window"),
      coalesce(dataset("start_date"), ds("start_date")).as("start_date"),
      coalesce(dataset("end_date"), ds("end_date")).as("end_date"),
      coalesce(dataset("total_records"), ds("total_records")).as("total_records"),
      coalesce(dataset("mean"), ds("mean")).as("mean"),
      coalesce(dataset("stdev"), ds("stdev")).as("stdev"),
      coalesce(dataset("certainty"), ds("certainty")).as("certainty"),
      coalesce(dataset("price_total"), ds("price_total")).as("price_total"),
      coalesce(dataset("price_above"), ds("price_above")).as("price_above"),
      coalesce(dataset("price_below"), ds("price_below")).as("price_below"),
      coalesce(dataset("regression_slope"), ds("regression_slope")).as("regression_slope"),
      coalesce(dataset("regression_intercept"), ds("regression_intercept")).as("regression_intercept"),
      coalesce(dataset("price_depriciation_total"), ds("price_depriciation_total")).as("price_depriciation_total"),
      coalesce(dataset("miles_depriciation"), ds("miles_depriciation")).as("miles_depriciation"),
      coalesce(dataset("total_miles"), ds("total_miles")).as("total_miles")
    )
    joined.withColumn("id", monotonically_increasing_id()).write
     .mode(SaveMode.Overwrite)
     .jdbc(this.getJdbcUrl, "summary_by_state_new", this.getConnectionProperties)
  }
  
  def exportSummaryOverTime(dataset: Dataset[SummaryOverTime]) : Unit = {
    import session.spark.implicits._
    var ds = session.spark.emptyDataset[SummaryOverTime]
    try {
      ds = session.spark.read.jdbc(this.getJdbcUrl, "summary_over_time", this.getConnectionProperties)
      .select("ymmt_id", "year_month", "total_records", "price_total")
      .as[SummaryOverTime]
    } catch {
      case sqlSyntaxErr: SQLSyntaxErrorException => {
        ds = session.spark.emptyDataset[SummaryOverTime]
      }
    }
    val joined = ds.join(right = dataset, usingColumns = Seq("ymmt_id", "year_month"), joinType = "full").select(
      col("ymmt_id"), col("year_month"),
      coalesce(dataset("total_records"), ds("total_records")).as("total_records"),
      coalesce(dataset("price_total"), ds("price_total")).as("price_total")
    )
    joined.withColumn("id", monotonically_increasing_id()).write
     .mode(SaveMode.Overwrite)
     .jdbc(this.getJdbcUrl, "summary_over_time_new", this.getConnectionProperties)
  }
}