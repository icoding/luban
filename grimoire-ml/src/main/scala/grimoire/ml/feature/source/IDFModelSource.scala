package grimoire.ml.feature.source

import grimoire.ml.source.MLReaderSource
import org.apache.spark.ml.feature.IDFModel
import org.apache.spark.ml.util.MLReader
import play.api.libs.json.JsValue

/**
  * Created by caphael on 2017/2/13.
  */
class IDFModelSource extends MLReaderSource[IDFModel]{
  override protected val reader: MLReader[IDFModel] = IDFModel.read
}

object IDFModelSource{

  def apply(json: JsValue): IDFModelSource =
    new IDFModelSource().parseJson(json)
}
