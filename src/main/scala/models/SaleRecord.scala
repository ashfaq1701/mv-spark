package models

import java.sql.Date

case class SaleRecord (vin: String, date: Date, price: Long, miles: Long, zip: Integer, ymmt_id: String, year_month: String)