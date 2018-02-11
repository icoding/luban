package grimoire.ml.transform

import grimoire.ml.configuration.param.{HasBreaks, HasLabels, HasRightClose}
import grimoire.spark.transform.dataframe.DataFrameUDFSpell
import grimoire.util.truansform.Cutter
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

/**
  * Created by caphael on 2017/3/28.
  */
class DataFrameCutSpell extends DataFrameUDFSpell with HasBreaks with HasLabels with HasRightClose{

  override def setup(dat: DataFrame): Boolean = {
    val cutter = new Cutter($(breaks),$(labels),$(rightClose))
    setUDF{
      (dat:Double)=>
        cutter(dat)
    }
    true
  }
}

object DataFrameCutSpell{
  def apply(json: JsValue): DataFrameCutSpell = new DataFrameCutSpell().parseJson(json)
}
