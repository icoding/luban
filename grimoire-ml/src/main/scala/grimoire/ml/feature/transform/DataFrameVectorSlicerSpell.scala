package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.ml.configuration.param.{HasIndices, HasNames}
import grimoire.spark.transform.dataframe.DataFrameSpell

/**
  * Created by sjc505 on 17-6-23.
  */
class DataFrameVectorSlicerSpell extends DataFrameSpell with HasInputCol with HasOutputCol
with HasIndices with HasNames{

  val slicer = new VectorSlicer()

  override def setup(dat: DataFrame): Boolean = {
    slicer
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
      .setIndices($(indices).toArray)
      .setNames($(names).toArray)
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    slicer.transform(dat)
  }
}

object DataFrameVectorSlicerSpell{
  def apply(json: JsValue="""{}"""): DataFrameVectorSlicerSpell =
    new DataFrameVectorSlicerSpell().parseJson(json)
}