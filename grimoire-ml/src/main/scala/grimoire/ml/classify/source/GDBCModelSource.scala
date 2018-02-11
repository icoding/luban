package grimoire.ml.classify.source
/**
  * Created by Aron on 17-11-6.
  */
import grimoire.ml.source.MLReaderSource
import org.apache.spark.ml.classification.GBTClassificationModel
import org.apache.spark.ml.util.MLReader
import play.api.libs.json.JsValue
import grimoire.Implicits._

class GDBCModelSource extends MLReaderSource[GBTClassificationModel]{
  override protected val reader: MLReader[GBTClassificationModel] = GBTClassificationModel.read
}

object GDBCModelSource{

  def apply(json: JsValue="""{}"""): GDBCModelSource =
    new GDBCModelSource().parseJson(json)
}