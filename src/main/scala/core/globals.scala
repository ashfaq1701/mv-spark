package core

import java.io.File

object globals {
  def deleteFolder(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteFolder)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }
}