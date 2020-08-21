import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Example extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
    .setAppName("sparkConf")
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("example")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val telecom = Parameters.incSchema(Telec.structType, "telecom", "\\t")

  telecom.select(
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
        col("TIME_INTERVAL") < "17:00:00",1)
      .otherwise(0))
    .groupBy(
      col("SQUARE_ID"),
      col("INTERVAL")
    ).agg(
    sum(col("SUM_ACTIVITY_USER")).as("ALL_ACTIVITY"),
    min(col("SUM_ACTIVITY_USER")).as("MIN_ACTIVITY"),
    max(col("SUM_ACTIVITY_USER").as("MAX_ACTIVITY")),
    avg(col("SUM_ACTIVITY_USER").as("AVG_ACTIVITY"))
  ).show()

}
