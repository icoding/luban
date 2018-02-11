package grimoire.ml.classify.source

import grimoire.source.SourceFromFile
import grimoire.spark.globalSpark
import org.apache.spark.mllib.classification.SVMModel
import play.api.libs.json.JsValue

/**
  * Created by caphael on 2017/3/27.
  */
class SVMModelSource extends SourceFromFile[SVMModel]{
//  override type ValueType = SVMModel

  override def conjureImpl: SVMModel = {
    SVMModel.load(globalSpark.sparkContext,$(inputPath))
  }
}

object SVMModelSource {
  def apply(json: JsValue): SVMModelSource = new SVMModelSource().parseJson(json)
}