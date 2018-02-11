package grimoire.ml.filtering.source
/**
  * Created by Aron on 17-11-8.
  */
import grimoire.ml.source.MLReaderSource
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.ml.util.MLReader
import play.api.libs.json.JsValue
import grimoire.Implicits._

class ALSModelSource extends MLReaderSource[ALSModel]{
  override protected val reader: MLReader[ALSModel] = ALSModel.read
}

object ALSModelSource{

  def apply(json: JsValue="""{}"""): ALSModelSource =
    new ALSModelSource().parseJson(json)
}

