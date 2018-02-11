package grimoire.spark.transform.dataframe

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import grimoire.common.transform.{LineSplitLike, LineSplitSpell}
import grimoire.transform.HasSingleSpellLike
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

/**
  * Created by caphael on 2017/1/5.
  */
class DataFrameLineSplitSpell extends DataFrameUDFSpell
  with HasSingleSpellLike[String,Seq[String]]
  with LineSplitLike[DataFrame,DataFrame]
  with HasInputCol
  with HasOutputCol
{

  override val single = new LineSplitSpell

  override def setup(dat: DataFrame): Boolean = {
    setUDF{
      (s:String) =>
        single.transformImpl(s)
    }
    super.setup(dat)
  }

}

object DataFrameLineSplitSpell {

  def apply(json:JsValue):DataFrameLineSplitSpell =
    new DataFrameLineSplitSpell().parseJson(json)
}