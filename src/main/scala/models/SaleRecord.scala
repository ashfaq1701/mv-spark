package models

import java.sql.Date

case class SaleRecord (vin: String, date: Date, price: Long, miles: Long, zip: Integer, ymmt_id: String, state: String, year_month: String)