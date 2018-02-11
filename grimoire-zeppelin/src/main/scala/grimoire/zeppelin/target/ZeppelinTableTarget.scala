package grimoire.zeppelin.target

import grimoire.Implicits.jstr2JsValue
import grimoire.configuration.param.HasOutputCols
import grimoire.target.Target
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.zeppelin.Implicits._
/**
  * Created by caphael on 2017/7/13.
  */
class ZeppelinTableTarget extends Target[DataFrame] with HasOutputCols{

  override def transformImpl(dat: DataFrame): Unit = {
    print(dat.setOutputCols($(outputCols)).toScript())
  }
}

object ZeppelinTableTarget{
  def apply(json:JsValue="""{}"""): ZeppelinTableTarget = new ZeppelinTableTarget().parseJson(json)
}