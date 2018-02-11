package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol, HasStopWordsPath}
import grimoire.spark.transform.dataframe.DataFrameSpell
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._

import scala.io.Source

/**
  * Created by Arno on 17-10-17.
  */
class DataFrameStopWordsRemoverSpell extends DataFrameSpell with HasInputCol
  with HasOutputCol with HasStopWordsPath{

  val remover = new StopWordsRemover()

  override def setup(dat: DataFrame): Boolean = {

    val stopWordList = Source.fromFile($(stopWordsPath)).getLines().toArray

    remover
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
      .setStopWords(stopWordList)
    super.setup(dat)
  }


  override def transformImpl(dat: DataFrame): DataFrame = {
    remover.transform(dat)
  }

}

object DataFrameStopWordsRemoverSpell{
  def apply(json: JsValue="""{}"""): DataFrameStopWordsRemoverSpell =
    new DataFrameStopWordsRemoverSpell().parseJson(json)
}