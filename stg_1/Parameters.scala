import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.StructType

object Parameters {
  val pathTelec = "./datasource/telec/*"

  def incSchema(structType: StructType, nameSchema: String, delimited: String)(implicit spark: SparkSession): DataFrame =
    spark.read.format("jdbc")
      .options(
        Map(
          "delimiter" -> delimited,
          "nullValue" -> "\\N"
        )
      ).schema(structType).csv(pathTelec)

}
