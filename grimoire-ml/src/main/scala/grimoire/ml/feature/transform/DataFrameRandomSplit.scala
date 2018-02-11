package grimoire.ml.feature.transform
/**
  * Created by Arno on 17-11-16.
  */
import grimoire.ml.configuration.param._
import grimoire.transform.Spell
import org.apache.spark.sql.DataFrame
import grimoire.Implicits._
import play.api.libs.json.JsValue

class DataFrameRandomSplit extends Spell[DataFrame,Array[org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]]] with HasRandomRate
  with TrainSet with TestSet{
  override def setup(dat: DataFrame): Boolean = {
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): Array[org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]] ={
    dat.randomSplit(Array($(randomRate),1-$(randomRate)))
  }
}

object DataFrameRandomSplit{
  def apply(json: JsValue="""{}"""): DataFrameRandomSplit =
    new DataFrameRandomSplit().parseJson(json)
}