package grimoire.ml.clustering.transform
import grimoire.dataset.&
import grimoire.transform.Spell
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import org.apache.spark.ml.clustering.BisectingKMeansModel
/**
  * Created by Arno on 17-11-8.
  */

class DataFrameBisectingKMeansPreSpell extends Spell[DataFrame & BisectingKMeansModel,DataFrame]{
  override def transformImpl(dat: &[DataFrame, BisectingKMeansModel]): DataFrame = {
    dat._2.transform(dat._1)
  }
}
object DataFrameBisectingKMeansPreSpell{
  def apply(json: JsValue="""{}"""): DataFrameBisectingKMeansPreSpell =
    new DataFrameBisectingKMeansPreSpell().parseJson(json)
}
