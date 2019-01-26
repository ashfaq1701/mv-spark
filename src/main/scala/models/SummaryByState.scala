package models

import java.sql.Date

case class SummaryByState(ymmt_id: String, start_date: Date, end_date: Date, state: String,
    total_records: Long, mean: Double, stdev: Double, certainty: Double, price_total: Long, 
    price_above: Double, price_below: Double, regression_slope: Double, 
    regression_intercept: Double, price_depriciation_total: Long, miles_depriciation: Long,
    total_miles: Long, month_window: Integer)