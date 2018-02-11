package grimoire.ml.classify.source
/**
  * Created by Aron on 17-11-6.
  */
import grimoire.ml.source.MLReaderSource
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.util.MLReader
import play.api.libs.json.JsValue
import grimoire.Implicits._

class LogisticRegressionModelSource extends MLReaderSource[LogisticRegressionModel]{
  override protected val reader: MLReader[LogisticRegressionModel] = LogisticRegressionModel.read
}

object LogisticRegressionModelSource{

  def apply(json: JsValue="""{}"""): LogisticRegressionModelSource =
    new LogisticRegressionModelSource().parseJson(json)
}
