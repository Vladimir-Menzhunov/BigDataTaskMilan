import Domain.{MiGrid, MiPollution, MiPollutionLegend, Telec}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{max, _}


object Example extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("example")
    .master("local[*]")
    .getOrCreate()
  spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
  //import spark.implicits._
  val telecom = Parameters.incSchema(Telec.structType, "\\t", Parameters.pathTelec)
    .select(
      col("SQUARE_ID"),
      from_unixtime(lit(col("TIME_INTERVAL") / 1000), "HH:mm:ss").as("TIME_INTERVAL"),
      (col("SMS_IN") +
        col("SMS_OUT") +
        col("CALL_IN") +
        col("CALL_OUT") +
        col("INTERNET_TRAFFIC")).as("SUM_ACTIVITY_USER")
    ).withColumn(
    "INTERVAL",
    when(
      col("TIME_INTERVAL") > "09:00:00" &&
        col("TIME_INTERVAL") < "17:00:00", 1)
      .otherwise(0))
    .groupBy(
      col("SQUARE_ID").as("SQUARE_ID_TELECOM"),
      col("INTERVAL")
    ).agg(
    sum(col("SUM_ACTIVITY_USER")).as("ALL_ACTIVITY"),
    min(col("SUM_ACTIVITY_USER")).as("MIN_ACTIVITY"),
    max(col("SUM_ACTIVITY_USER")).as("MAX_ACTIVITY"),
    avg(col("SUM_ACTIVITY_USER")).as("AVG_ACTIVITY")
  )
  //stg_2

  val miGrid = Parameters.incSchema(MiGrid.structType, ",", Parameters.pathMiGrid)

  val miGrid_telec = telecom
    .join(miGrid, telecom("SQUARE_ID_TELECOM") === miGrid("SQUARE_ID"), "inner")

  //stg_3
  //  val pollution = Parameters.incSchema(MiPollution.structType, ",", Parameters.pathMiPollution)
  //    .groupBy(col("SENSOR_ID")).agg(max(col("MEASUREMENT")).as("MAX_MEASUREMENT"))
  //
  //    .show()
  //  На этапе STG_3 необходимо объединить данные MI_POLLUTION/LEGEND
  //  и MI_POLLUTION/POLLUTION_MI для получения информации о загрязнениях по sensor id.
  val pollution = Parameters.incSchema(MiPollution.structType, ",", Parameters.pathMiPollution)
    .select(
      col("SENSOR_ID"),
      col("MEASUREMENT"),
      when(hour(to_timestamp(col("TIME_INSTANT"), "yyyy/dd/mm HH:mm").cast("timestamp")) >= 9 &&
        hour(to_timestamp(col("TIME_INSTANT"), "yyyy/dd/mm HH:mm").cast("timestamp")) <= 17, 1)
        .otherwise(0).as("INTERVAL"))
    .groupBy(
      col("SENSOR_ID"),
      col("INTERVAL")
    ).agg(max(col("MEASUREMENT")).as("MAX_MEASUREMENT"))

  val pollutionLegend = Parameters.incSchema(MiPollutionLegend.structType, ",", Parameters.pathMiPollutionLegend)

  val pollLegend_pollution = pollutionLegend.as("pl").join(pollution.as("p"), pollution("SENSOR_ID") === pollutionLegend("SENSOR_ID"), "inner")
    .select(
      col("p.SENSOR_ID").as("SENSOR_ID"),
      col("p.INTERVAL").as("INTERVAL_POLL"),
      col("p.MAX_MEASUREMENT").as("MAX_MEASUREMENT"),
      col("pl.STREET_NAME").as("STREET_NAME"),
      col("pl.SENSOR_LAT").as("SENSOR_LAT"),
      col("pl.SENSOR_LONG").as("SENSOR_LONG"),
      col("pl.SENSOR_TYPE").as("SENSOR_TYPE")
    )

  //stg_4
  //  На этапе STG_4 необходимо объединить данные с этапов STG_2 и STG_3
  //  для получения отчета по активности и загрязнениям по Square id.
  //    Определить загрязненность и активность рабочей и жилой зон.

  val list_zone_pollution = miGrid_telec.join(
    pollLegend_pollution,
    miGrid_telec("SQUARE_ID") === pollLegend_pollution("SENSOR_ID"), "inner"
  ).filter(col("INTERVAL") === col("INTERVAL_POLL")
  ).select(
      col("SQUARE_ID"),
      col("INTERVAL"),
      col("MAX_MEASUREMENT"),
      col("SENSOR_TYPE"),
      col("ALL_ACTIVITY"),
      col("MIN_ACTIVITY"),
      col("MAX_ACTIVITY"),
      col("AVG_ACTIVITY")
    )

  //Топ 5 «загрязненных» рабочих и спальных зон.
  val list_most_pollution_zone =
    list_zone_pollution.orderBy(col("MAX_MEASUREMENT").desc).limit(5)

  // Топ 5 «чистых» рабочих и спальных зон.
  val list_clear_pollution_zone =
    list_zone_pollution.orderBy(col("MAX_MEASUREMENT").asc).limit(5)

//  список не определенных по классу или по уровню загрязнения зон
  val list_undefined_pollution = miGrid_telec
    .join(
      pollLegend_pollution,
      miGrid_telec("SQUARE_ID") === pollLegend_pollution("SENSOR_ID"), "left"
    ).filter(col("SENSOR_ID").isNull).select(
    col("SQUARE_ID"),
    col("INTERVAL"),
    col("ALL_ACTIVITY"),
    col("MIN_ACTIVITY"),
    col("MAX_ACTIVITY"),
    col("AVG_ACTIVITY")
  )


}
