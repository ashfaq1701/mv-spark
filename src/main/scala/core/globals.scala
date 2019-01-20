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
}