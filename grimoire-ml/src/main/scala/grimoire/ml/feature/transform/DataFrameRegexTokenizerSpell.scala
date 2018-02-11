package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.ml.configuration.param.{HasGaps, HasMintokenLength, HasPattern, HasToLowercase}
import grimoire.spark.transform.dataframe.DataFrameSpell

/**
  * Created by Arno on 17-10-17.
  */
class DataFrameRegexTokenizerSpell extends DataFrameSpell with HasInputCol
  with HasOutputCol with HasPattern  with HasMintokenLength with HasToLowercase{
  val regexTokenizer = new RegexTokenizer()
  pattern.default("\\s+")
  minTokenLength.default(1)
  toLowercase.default(true)

  override def setup(dat: DataFrame): Boolean = {
    regexTokenizer
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
      .setPattern($(pattern))
      .setMinTokenLength($(minTokenLength))
      .setToLowercase($(toLowercase))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    regexTokenizer.transform(dat)
  }

}

object DataFrameRegexTokenizerSpell{
  def apply(json: JsValue="""{}"""): DataFrameRegexTokenizerSpell
  = new DataFrameRegexTokenizerSpell().parseJson(json)
}