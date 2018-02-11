package grimoire.ml.clustering.source

import grimoire.ml.configuration.param.HasIsLocalModel
import grimoire.ml.source.MLReaderSource
import org.apache.spark.ml.clustering.{DistributedLDAModel, LDAModel, LocalLDAModel}
import org.apache.spark.ml.util.MLReader
import play.api.libs.json.JsValue

/**
  * Created by caphael on 2017/3/22.
  */
class LDAModelSource extends MLReaderSource[LDAModel]
  with HasIsLocalModel{
  override protected val reader: MLReader[LDAModel] = null

  override def conjureImpl: LDAModel = {
    if ($(isLocalModel))
      LocalLDAModel.read.load($(inputPath))
    else
      DistributedLDAModel.read.load($(inputPath))
  }
}

object LDAModelSource {
  def apply(json: JsValue): LDAModelSource =
    new LDAModelSource().parseJson(json)
}