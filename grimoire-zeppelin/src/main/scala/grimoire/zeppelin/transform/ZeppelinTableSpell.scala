package grimoire.zeppelin.transform

import grimoire.Implicits.jstr2JsValue
import grimoire.configuration.param.HasOutputCols
import grimoire.transform.Spell
import org.apache.spark.sql.DataFrame
import grimoire.zeppelin.Implicits._
import play.api.libs.json.JsValue

/**
  * Created by caphael on 2017/7/13.
  */
class ZeppelinTableSpell extends Spell[DataFrame,String] with HasOutputCols{

  override def transformImpl(dat: DataFrame): String = {
    dat.setOutputCols($(outputCols)).toScript()
  }
}

object ZeppelinTableSpell{
  def apply(json:JsValue="""{}"""): ZeppelinTableSpell = new ZeppelinTableSpell().parseJson(json)
}