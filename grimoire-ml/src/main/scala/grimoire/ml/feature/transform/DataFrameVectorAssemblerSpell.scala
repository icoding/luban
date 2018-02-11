package grimoire.ml.feature.transform

import grimoire.configuration.param.HasOutputCol
import grimoire.ml.configuration.param.HasInputCols
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.spark.transform.dataframe.DataFrameSpell


/**
  * Created by sjc505 on 17-6-23.
  */
class DataFrameVectorAssemblerSpell extends DataFrameSpell
  with HasInputCols with HasOutputCol{
  val assembler = new VectorAssembler()

  override def setup(dat: DataFrame): Boolean = {
    assembler
      .setInputCols($(inputCols).toArray)
      .setOutputCol($(outputCol))
    super.setup(dat)
  }
  override def transformImpl(dat: DataFrame): DataFrame = {
    assembler.transform(dat)
  }
}

object DataFrameVectorAssemblerSpell{
  def apply(json: JsValue="""{}"""): DataFrameVectorAssemblerSpell =
    new DataFrameVectorAssemblerSpell().parseJson(json)
}
