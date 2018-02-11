package grimoire.spark.serializer
import grimoire.util.json.writer.JsonWriters
import org.apache.spark.sql.Row
import play.api.libs.json.JsObject

/**
  * Created by caphael on 2017/3/23.
  */
class RowJsonSerializer extends RowSerializer[String] {
  override def serialize(dat: Row): String = {
    val schema = dat.schema

    val jsvs = schema.map{
      case f=>
        (f.name,f.dataType.typeName.toLowerCase)
    }.zip(dat.toSeq).map{
      case ((n,t),v) =>
        JsonWriters.get(t).write(n,v)
    }

    JsObject(jsvs).toString()
  }
}
