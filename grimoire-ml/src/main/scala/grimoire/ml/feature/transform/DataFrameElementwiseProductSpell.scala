package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.feature.ElementwiseProduct
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.ml.configuration.param.HasScalingVec
import grimoire.spark.transform.dataframe.DataFrameSpell
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by sjc505 on 17-6-23.
  */
class DataFrameElementwiseProductSpell extends  DataFrameSpell
  with HasInputCol with HasOutputCol with HasScalingVec{
  val transformer = new ElementwiseProduct()

  override def setup(dat: DataFrame): Boolean = {
    transformer
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
      .setScalingVec(Vectors.dense($(scalingVec).toArray))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    transformer.transform(dat)
  }

}

object DataFrameElementwiseProductSpell{
  def apply(json: JsValue="""{}"""): DataFrameElementwiseProductSpell =
    new DataFrameElementwiseProductSpell().parseJson(json)
}
