import java.io.{BufferedWriter, FileWriter}

import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.io.Source

object JsonDecoder extends App {
  val source: String =
    Source.fromFile("./datasource/milano-grid.geojson")
      .getLines
      .mkString

  val file = "./datasource/new_file_grid.csv"
  val writer = new BufferedWriter(new FileWriter(file))

  val json = parse(source) \ "features"

  val cellId = for {
    JObject(properties) <- json
    JField("cellId", JInt(cellId)) <- properties
  } yield cellId

  val coor = for {
    JObject(geometry) <- json
    JField("coordinates", JArray(coordinates)) <- geometry
  } yield coordinates

  val miGrid = cellId.zip(
    coor.map {
      case List(JArray(list)) =>
        list.map {
          case JArray(list1) => list1
        }
      case _ => None
    }).map {
    case (square_id, coordinates) => (square_id, "[0-9]*\\.[0-9]*".r.findAllIn(coordinates.toString).mkString(","))
  }

  Range(1, 10000).zip(miGrid.map {
    case (square_id, str) => writer.write(s"$square_id,$str\n")
  })
  writer.close()
}

