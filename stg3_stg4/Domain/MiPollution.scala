package Domain
import org.apache.spark.sql.types._

object MiPollution extends Enumeration {
  val SENSOR_ID, TIME_INSTANT, MEASUREMENT = Value

  val structType = StructType(
    Seq(
      StructField(SENSOR_ID.toString, IntegerType),
      StructField(TIME_INSTANT.toString, StringType),
      StructField(MEASUREMENT.toString, DoubleType)
    )
  )

}
