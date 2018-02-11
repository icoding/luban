package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import grimoire.ml.configuration.param.HasN
import org.apache.spark.ml.feature.NGram
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.spark.transform.dataframe.DataFrameSpell

/**
  * Created by sjc505 on 17-6-21.
  */
class DataFrameNGramSpell extends DataFrameSpell with HasInputCol
  with HasOutputCol with HasN{

  val ngram = new NGram()
  n.default(2)

  override def setup(dat: DataFrame): Boolean = {
    ngram
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
      .setN($(n))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    ngram.transform(dat)
  }
}

object DataFrameNGramSpell{
  def apply(json: JsValue="""{}"""): DataFrameNGramSpell =
    new DataFrameNGramSpell().parseJson(json)
}
