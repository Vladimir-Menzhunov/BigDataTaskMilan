import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

import scala.io.Source
import scala.util.Try

object JsonDecoder extends App{

    val source: String =
        Source.fromFile("./datasource/milano-grid.geojson")
          .getLines
          .mkString

    val json = parse(source) \ "features"

   val cellId = for {
        JObject(properties) <- json
        JField("cellId", JInt(cellId)) <- properties
    } yield cellId

   val coor = for {
       JObject(geometry) <- json
       JField("coordinates", JArray(coordinates)) <- geometry
   } yield coordinates

    implicit val formats = DefaultFormats

    println(cellId.zip(coor))





}
