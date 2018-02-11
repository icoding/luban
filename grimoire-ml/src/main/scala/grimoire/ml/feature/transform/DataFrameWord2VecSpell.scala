package grimoire.ml.feature.transform

import grimoire.ml.configuration.param.{HasMinCount, HasVectorSize, HasMaxIter}
import org.apache.spark.ml.feature.Word2Vec
import grimoire.Implicits._
import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import grimoire.spark.transform.dataframe.DataFrameSpell
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue


/**
  * Created by Arno on 17-10-17.
  */
class DataFrameWord2VecSpell extends DataFrameSpell with HasInputCol
  with HasOutputCol with HasVectorSize with HasMinCount with HasMaxIter{
  maxIter.default(1)


  val word2Vec = new Word2Vec()

  override def setup(dat: DataFrame): Boolean = {
    word2Vec
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
      .setVectorSize($(vectorsize))
      .setMinCount($(mincount))
      .setMaxIter($(maxIter))
    super.setup(dat)

  }

  override def transformImpl(dat: DataFrame): DataFrame= {
    val model = word2Vec.fit(dat)
    model.transform(dat)
  }
}


object DataFrameWord2VecSpell{
  def apply(json: JsValue="""{}"""): DataFrameWord2VecSpell =
    new DataFrameWord2VecSpell().parseJson(json)
}