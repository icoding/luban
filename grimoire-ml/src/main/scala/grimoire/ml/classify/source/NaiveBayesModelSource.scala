package grimoire.ml.classify.source
/**
  * Created by Aron on 17-11-6.
  */
import grimoire.ml.source.MLReaderSource
import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.ml.util.MLReader
import play.api.libs.json.JsValue
import grimoire.Implicits._

class NaiveBayesModelSource extends MLReaderSource[NaiveBayesModel]{
  override protected val reader: MLReader[NaiveBayesModel] = NaiveBayesModel.read
}

object NaiveBayesModelSource{

  def apply(json: JsValue="""{}"""): NaiveBayesModelSource =
    new NaiveBayesModelSource().parseJson(json)
}
