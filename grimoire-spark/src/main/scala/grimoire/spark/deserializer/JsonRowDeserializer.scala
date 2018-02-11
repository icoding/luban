package grimoire.spark.deserializer

import grimoire.util.deserializer.DataDeserializer
import grimoire.Implicits.jstr2JsValue
import grimoire.util.json.reader.JsonReaders
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import play.api.libs.json.Json


/**
  * Created by caphael on 2016/12/25.
  */
class JsonRowDeserializer(structType:StructType) extends RowDeserializer[String] {

  private var updateSchema = true

  def discardSchema():this.type = {
    updateSchema = false
    this
  }

  override def deserialize(dat: String): Row = {
    val jsv = Json.parse(dat)
    val cols = dfSchema.map{
      case f=>
        JsonReaders.get(f.dataType.typeName.toLowerCase).read(jsv,f.name)
    }
    if (updateSchema) {
      new GenericRowWithSchema(cols.toArray,dfSchema)
    }else {
      Row(cols:_*)
    }
  }

  override val dfSchema: StructType = structType
}

object JsonRowDeserializer{
  def apply(structType: StructType): JsonRowDeserializer = new JsonRowDeserializer(structType)
}