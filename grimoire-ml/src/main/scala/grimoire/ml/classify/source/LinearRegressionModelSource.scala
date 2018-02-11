package grimoire.ml.classify.source
/**
  * Created by Aron on 17-11-6.
  */
import grimoire.ml.source.MLReaderSource
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.ml.util.MLReader
import play.api.libs.json.JsValue

class LinearRegressionModelSource extends MLReaderSource[LinearRegressionModel]{
  override protected val reader: MLReader[LinearRegressionModel] = LinearRegressionModel.read
}

object LinearRegressionModelSource{
  def apply(json: JsValue): LinearRegressionModelSource =
    new LinearRegressionModelSource().parseJson(json)
}