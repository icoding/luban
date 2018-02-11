package grimoire.ml.feature.transform

import grimoire.ml.configuration.param.HasFeatureCols
import grimoire.transform.{HasSingleSpellLike, Spell}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import play.api.libs.json.JsValue

/**
  * Created by caphael on 2017/3/23.
  */
private[grimoire] class DataFrameMLVectorizeSpell
  extends Spell[DataFrame,RDD[Vector]]
  with HasFeatureCols
  with HasSingleSpellLike[Row,Vector]{

  override def parseJson(json: JsValue): DataFrameMLVectorizeSpell.this.type = {
    single.parseJson(json)
    super.parseJson(json)
  }

  override val single: Spell[Row, Vector] = new RowToVectorSpell

  override def transformImpl(dat: DataFrame): RDD[Vector] = {
    dat.rdd.map{
      case r =>
        single.transform(r)
    }
  }
}

private[grimoire] object DataFrameMLVectorizeSpell{
  def apply(json: JsValue): DataFrameMLVectorizeSpell = new DataFrameMLVectorizeSpell().parseJson(json)
}
