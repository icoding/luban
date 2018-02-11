package grimoire.ml.classify.source
/**
  * Created by Aron on 17-11-6.
  */
import grimoire.ml.source.MLReaderSource
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.util.MLReader
import play.api.libs.json.JsValue
import grimoire.Implicits._

class DecisionTreeModelSource extends MLReaderSource[DecisionTreeClassificationModel]{
  override protected val reader: MLReader[DecisionTreeClassificationModel] = DecisionTreeClassificationModel.read
}

object DecisionTreeModelSource{

  def apply(json: JsValue="""{}"""): DecisionTreeModelSource =
    new DecisionTreeModelSource().parseJson(json)
}

