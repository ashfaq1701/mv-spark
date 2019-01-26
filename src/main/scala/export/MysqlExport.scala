package export

import org.apache.spark.sql.Dataset
import core.conf
import models.SummaryBase
import models.SummaryByState
import models.SummaryOverTime

object MysqlExport {
  val mysqlConf = conf.parseMysqlConf
  
  def exportSummaryBase(dataset: Dataset[SummaryBase]) : Unit = {
    
  }
  
  def exportSummaryByState(dataset: Dataset[SummaryByState]) : Unit = {
    
  }
  
  def exportSummaryOverTime(dataset: Dataset[SummaryOverTime]) : Unit = {
    
  }
}