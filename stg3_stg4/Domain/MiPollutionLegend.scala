package Domain

import org.apache.spark.sql.types._

object MiPollutionLegend extends Enumeration {
  val SENSOR_ID, STREET_NAME, SENSOR_LAT, SENSOR_LONG, SENSOR_TYPE, UOM, TIME_FORMAT = Value

  val structType = StructType(
    Seq(
      StructField(SENSOR_ID.toString, IntegerType),
      StructField(STREET_NAME.toString, StringType),
      StructField(SENSOR_LAT.toString, DoubleType),
      StructField(SENSOR_LONG.toString, DoubleType),
      StructField(SENSOR_TYPE.toString, StringType),
      StructField(UOM.toString, StringType),
      StructField(TIME_FORMAT.toString, StringType)
    )
  )


}
