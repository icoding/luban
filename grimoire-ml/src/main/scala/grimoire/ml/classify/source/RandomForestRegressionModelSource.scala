package grimoire.ml.classify.source
/**
  * Created by Aron on 17-11-8.
  */
import grimoire.ml.source.MLReaderSource
import org.apache.spark.ml.regression.RandomForestRegressionModel
import org.apache.spark.ml.util.MLReader
import play.api.libs.json.JsValue

class RandomForestRegressionModelSource extends MLReaderSource[RandomForestRegressionModel]{
  override protected val reader: MLReader[RandomForestRegressionModel] = RandomForestRegressionModel.read
}

object RandomForestRegressionModelSource{
  def apply(json: JsValue): RandomForestRegressionModelSource =
    new RandomForestRegressionModelSource().parseJson(json)
}