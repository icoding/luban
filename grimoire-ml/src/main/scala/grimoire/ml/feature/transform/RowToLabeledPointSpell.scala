package grimoire.ml.feature.transform

import grimoire.ml.configuration.param.{HasFeatureCols, HasLabelCol}
import grimoire.Implicits._
import grimoire.transform.Spell
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.sql.Row
import play.api.libs.json.JsValue

/**
  * Created by caphael on 2017/3/23.
  */

/**
  * For [[RowToLabeledPointSpell]], label must be Double.
  */
class RowToLabeledPointSpell
  extends Spell[Row,LabeledPoint]
  with HasLabelCol
  with HasFeatureCols{
  val vectorizer = new RowToVectorSpell

  var fcols : Seq[String] = null

  override def transformImpl(dat: Row): LabeledPoint = {
    if (fcols == null){
      fcols = if ($(featureCols).equals(Seq("*")))
        dat.schema.map{_.name}.filter(_ != $(labelCol))
      else
        $(featureCols)
    }

//    vectorizer.setFeatureCols(fcols)
    val v = vectorizer.setFeatureCols(fcols).transform(dat)
    val label = dat.getAs[Long]($(labelCol)).toDouble

    LabeledPoint(label,v)

  }

  override def parseJson(json: JsValue): RowToLabeledPointSpell.this.type = {
    vectorizer.parseJson(json)
    super.parseJson(json)
  }

}

object RowToLabeledPointSpell{
  def apply(json: JsValue): RowToLabeledPointSpell = new RowToLabeledPointSpell().parseJson(json)
}