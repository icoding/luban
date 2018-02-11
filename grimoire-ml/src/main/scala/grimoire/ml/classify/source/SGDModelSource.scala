package grimoire.ml.classify.source

/**
  * Created by jax on 17-6-29.
  */

import grimoire.source.SourceFromFile
import grimoire.spark.globalSpark
import org.apache.spark.mllib.classification.LogisticRegressionModel
import play.api.libs.json.JsValue

class SGDModelSource extends SourceFromFile[LogisticRegressionModel]{
  //  override type ValueType = SVMModel

  override def conjureImpl: LogisticRegressionModel = {
    LogisticRegressionModel.load(globalSpark.sparkContext,$(inputPath))
  }
}

object SGDModelSource {
  def apply(json: JsValue): SGDModelSource = new SGDModelSource().parseJson(json)
}