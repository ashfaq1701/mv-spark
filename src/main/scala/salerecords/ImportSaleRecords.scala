package salerecords


import core.database

object ImportSaleRecords {
  def importRecords (path : String): Unit = {
    database.createDatabase
  }
}