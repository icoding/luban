package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import grimoire.ml.configuration.param.HasK
import org.apache.spark.ml.feature.PCA
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.spark.transform.dataframe.DataFrameSpell

/**
  * Created by sjc505 on 17-6-21.
  */
class DataFramePCASpell extends DataFrameSpell with HasInputCol
  with HasOutputCol with HasK{

  val pca = new PCA()

  override def setup(dat: DataFrame): Boolean ={
    pca
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
      .setK($(k))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    val model=pca.fit(dat)
    model.transform(dat)
  }
}

object DataFramePCASpell{
  def apply(json: JsValue="""{}"""): DataFramePCASpell =
    new DataFramePCASpell().parseJson(json)
}
