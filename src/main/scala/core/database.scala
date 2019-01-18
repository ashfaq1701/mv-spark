package core

object database {
  def createDatabase : Unit = {
    if (session.spark.catalog.databaseExists("salerecords")) {
      session.spark.sql("CREATE DATABASE salerecords")
    }
  }
}