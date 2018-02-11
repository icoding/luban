package grimoire.ml.feature.transform

import org.apache.spark.ml.feature.CountVectorizer
import grimoire.Implicits._
import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import grimoire.ml.configuration.param.{HasBinary, HasMinDF, HasVocabSize}
import grimoire.spark.transform.dataframe.DataFrameSpell
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue


/**
  * Created by Arno on 17-10-17.
  */

class DataFrameCountVectorzerSpell extends DataFrameSpell with HasInputCol
  with HasOutputCol with HasVocabSize with HasMinDF with HasBinary{

  val cv: CountVectorizer = new CountVectorizer()
  minDF.default(1)
  vocabSize.default(262144)
  binary.default(false)

  override def setup(dat: DataFrame): Boolean = {
    cv
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
      .setVocabSize($(vocabSize))
      .setMinDF($(minDF))
      .setBinary($(binary))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    val model = cv.fit(dat)
    model.transform(dat)
  }
}


object DataFrameCountVectorzerSpell{
  def apply(json: JsValue="""{}"""): DataFrameCountVectorzerSpell =
    new DataFrameCountVectorzerSpell().parseJson(json)
}