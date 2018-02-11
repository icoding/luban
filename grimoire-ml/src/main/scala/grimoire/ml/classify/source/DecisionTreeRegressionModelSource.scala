package grimoire.ml.classify.source
/**
  * Created by Aron on 17-11-6.
  */
import grimoire.ml.source.MLReaderSource
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.util.MLReader
import play.api.libs.json.JsValue

class DecisionTreeRegressionModelSource extends MLReaderSource[DecisionTreeRegressionModel]{
  override protected val reader: MLReader[DecisionTreeRegressionModel] = DecisionTreeRegressionModel.read
}

object DecisionTreeRegressionModelSource{
  def apply(json: JsValue): DecisionTreeRegressionModelSource =
    new DecisionTreeRegressionModelSource().parseJson(json)
}