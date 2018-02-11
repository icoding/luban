package grimoire.spark.transform.single

import grimoire.spark.serializer.RowJsonSerializer
import grimoire.transform.Spell
import org.apache.spark.sql.Row
import play.api.libs.json.JsValue

/**
  * Created by caphael on 2017/3/23.
  */
class RowToJsonSpell extends Spell[Row,String]{
  val serializer = new RowJsonSerializer

  override def transformImpl(dat: Row): String = {
    serializer.serialize(dat)
  }
}

object RowToJsonSpell{
  def apply: RowToJsonSpell = new RowToJsonSpell()

  def apply(json: JsValue): RowToJsonSpell = new RowToJsonSpell().parseJson(json)
}
