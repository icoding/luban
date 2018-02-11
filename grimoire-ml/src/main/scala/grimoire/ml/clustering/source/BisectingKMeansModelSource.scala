package grimoire.ml.clustering.source

import grimoire.ml.source.MLReaderSource
import org.apache.spark.ml.clustering.BisectingKMeansModel
import org.apache.spark.ml.util.MLReader
import play.api.libs.json.JsValue
/**
  * Created by Arno on 2017/11/8.
  */
class BisectingKMeansModelSource extends MLReaderSource[BisectingKMeansModel]{
  override protected val reader: MLReader[BisectingKMeansModel] = BisectingKMeansModel.read
}

object BisectingKMeansModelSource{
  def apply(json: JsValue): BisectingKMeansModelSource =
    new BisectingKMeansModelSource().parseJson(json)
}