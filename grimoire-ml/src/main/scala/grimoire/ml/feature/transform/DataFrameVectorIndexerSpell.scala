package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import grimoire.ml.configuration.param.HasMaxCategories
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.spark.transform.dataframe.DataFrameSpell

/**
  * Created by sjc505 on 17-6-21.
  */
class DataFrameVectorIndexerSpell extends DataFrameSpell with HasInputCol
  with HasOutputCol with HasMaxCategories{

  val indexer = new VectorIndexer()
  maxCategories.default(10)
  override protected def setup(dat: DataFrame): Boolean ={
    indexer
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
      .setMaxCategories($(maxCategories))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    val model=indexer.fit(dat)
    model.transform(dat)
  }

}

object DataFrameVectorIndexerSpell{
  def apply(json: JsValue="""{}"""): DataFrameVectorIndexerSpell =
    new DataFrameVectorIndexerSpell().parseJson(json)
}

