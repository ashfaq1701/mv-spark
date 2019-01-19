package core

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField

object schemas {
  val ymmtSchema = StructType(
    StructField("vin_prefix", StringType, false) ::
    StructField("ymmt_id", StringType, false) :: Nil
  )
}