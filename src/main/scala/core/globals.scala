package core

import java.io.File
import java.sql.Date
import java.util.Calendar
import java.text.SimpleDateFormat

object globals {
  def deleteFolder(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteFolder)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }
  
  def minusMonths (date: Date, months: Integer): String = {
    val cal = Calendar.getInstance
    val df = new SimpleDateFormat("yyyy-MM-dd")
    cal.setTime(date)
    cal.add(Calendar.MONTH, 0 - months)
    df.format(cal.getTime)
  }
  
  def colMap(table1: String, table2: String, col: String, cols: String*): String = {
    var ons = new Array[String](cols.length + 1)
    ons(0) = table1 + "." + col + " = " + table2 + "." + col
    for (i <- 0 until cols.length) {
      ons(i + 1) = table1 + "." + cols(i) + " = " + table2 + "." + cols(i)
    }
    "(" + ons.mkString(" AND ") + ")"
  }
  
  def columns (col: String, cols: String*): String = {
    var columns = new Array[String](cols.length + 1)
    columns(0) = col
    for (i <- 0 until cols.length) {
      columns(i + 1) = cols(i)
    }
    columns.mkString(",")
  }
}