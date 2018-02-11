package grimoire.spark.transform.single

import grimoire.configuration.param.HasSchema
import grimoire.spark.deserializer.JsonRowDeserializer
import grimoire.transform.Spell
import org.apache.spark.sql.Row
import play.api.libs.json.JsValue

/**
  * Created by caphael on 2017/3/21.
  */
class JsonToRowSpell extends Spell[String,Row] with HasSchema{
  var deserializer:JsonRowDeserializer = null

  schema.action{
    case _ =>
      deserializer = JsonRowDeserializer($(schema))
  }

  override def transformImpl(dat: String): Row = {
    deserializer.deserialize(dat)
  }
}

object JsonToRowSpell extends JsonToRowSpell{
  def apply(json: JsValue): JsonToRowSpell = new JsonToRowSpell().parseJson(json)
}