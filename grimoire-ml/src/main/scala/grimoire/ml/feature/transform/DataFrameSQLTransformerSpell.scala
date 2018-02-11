package grimoire.ml.feature.transform

import grimoire.configuration.param.Param
import grimoire.ml.configuration.param.HasStatement
import org.apache.spark.ml.feature.SQLTransformer
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.spark.transform.dataframe.DataFrameSpell

/**
  * Created by sjc505 on 17-6-23.
  */
class DataFrameSQLTransformerSpell extends DataFrameSpell with HasStatement {

  val sqlTrans = new SQLTransformer()

  override def setup(dat: DataFrame): Boolean = {
    sqlTrans.setStatement($(statement))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    sqlTrans.transform(dat)
  }
}

object DataFrameSQLTransformerSpell{
  def apply(json: JsValue="""{}"""): DataFrameSQLTransformerSpell =
    new DataFrameSQLTransformerSpell().parseJson(json)
}

