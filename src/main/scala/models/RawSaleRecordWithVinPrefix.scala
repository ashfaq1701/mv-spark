package models

import java.sql.Date

case class RawSaleRecordWithVinPrefix (vin: String, date: Date, price: Long, miles: Long, zip: Integer, vin_prefix: String)