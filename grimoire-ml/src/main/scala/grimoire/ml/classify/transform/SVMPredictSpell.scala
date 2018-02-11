package grimoire.ml.classify.transform

import grimoire.configuration.param.HasOutputCol
import grimoire.dataset.&
import grimoire.ml.feature.transform.RowToVectorSpell
import grimoire.ml.Implicits._
import grimoire.Implicits._
import grimoire.ml.configuration.param.HasFeatureCols
import grimoire.transform.Spell
import org.apache.spark.sql.Row
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.DoubleType
import play.api.libs.json.JsValue

/**
  * Created by caphael on 2017/3/27.
  */
class SVMPredictSpell extends Spell[Row & SVMModel,Row] with HasOutputCol with HasFeatureCols{

  private var updateSchema:Boolean = true
  def discardSchema():this.type = {
    updateSchema = false
    this
  }

  val vectorizer = new RowToVectorSpell()
  outputCol.default("prediction")

  featureCols.action{
    case _ =>
      vectorizer.setFeatureCols($(featureCols))
  }

  override def transformImpl(dat: Row & SVMModel): Row = {
    val vec = vectorizer.transform(dat._1)
    val pred = dat._2.predict(vec)

    if (updateSchema){
      val schema = dat._1.schema.add($(outputCol),DoubleType)
      new GenericRowWithSchema((dat._1.toSeq :+ pred).toArray,schema)
    }
    else{
      Row((dat._1.toSeq :+ pred):_*)
    }
  }

  override def parseJson(json: JsValue): SVMPredictSpell.this.type = {
    vectorizer.parseJson(json)
    super.parseJson(json)
  }
}

object SVMPredictSpell{
  def apply(json: JsValue): SVMPredictSpell = new SVMPredictSpell().parseJson(json)
}