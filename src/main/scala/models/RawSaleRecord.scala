package models

import java.sql.Date

case class RawSaleRecord (vin: String, date: Date, price: Long, miles: Long, zip: Integer)