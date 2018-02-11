package grimoire.ml.feature.transform

/**
  * Created by caphael on 2017/3/23.
  */
import grimoire.ml.configuration.param.HasFeatureCols
import grimoire.transform.Spell
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.Row
import play.api.libs.json.JsValue

class RowToVectorSpell
  extends Spell[Row,Vector]
  with HasFeatureCols {

  override def transformImpl(dat: Row): Vector = {
    val values = dat.getValuesMap[Double]($(featureCols)).values.map{
      case d:Double => d
    }
    Vectors.dense(values.toArray)
  }
}

object RowToVectorSpell{
  def apply(json: JsValue): RowToVectorSpell = new RowToVectorSpell().parseJson(json)
}