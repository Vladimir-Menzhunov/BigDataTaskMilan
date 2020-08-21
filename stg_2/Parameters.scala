import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.StructType

object Parameters {
  val pathTelec = "./datasource/telec/*"
  val pathMiGrid = "./datasource/new_file_grid.csv"

  def incSchema(structType: StructType, delimited: String, path: String)(implicit spark: SparkSession): DataFrame =
    spark.read
      .options(
        Map(
          "delimiter" -> delimited,
          "nullValue" -> "\\N"
        )
      ).schema(structType).csv(path)

}
