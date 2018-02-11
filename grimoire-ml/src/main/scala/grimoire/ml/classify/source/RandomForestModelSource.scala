package grimoire.ml.classify.source
/**
  * Created by Aron on 17-11-6.
  */
import grimoire.ml.source.MLReaderSource
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.util.MLReader
import play.api.libs.json.JsValue
import grimoire.Implicits._

class RandomForestModelSource extends MLReaderSource[RandomForestClassificationModel]{
  override protected val reader: MLReader[RandomForestClassificationModel] = RandomForestClassificationModel.read
}

object RandomForestModelSource{

  def apply(json: JsValue="""{}"""): RandomForestModelSource =
    new RandomForestModelSource().parseJson(json)
}
