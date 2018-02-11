package grimoire.ml.feature.transform

/**
  * Created by jax on 17-6-27.
  */


import grimoire.ml.configuration.param._
import grimoire.transform.Spell
import org.apache.spark.sql.DataFrame
import grimoire.Implicits._
import grimoire.dataset.&
import play.api.libs.json.JsValue

class DataFrameRandomSplitSpell extends Spell[DataFrame,DataFrame & DataFrame] with HasRandomRate{

  override def setup(dat: DataFrame): Boolean = {
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame & DataFrame ={
    val Array(a,b) =dat.randomSplit(Array($(randomRate),1-$(randomRate)))
    a & b
  }
}

object DataFrameRandomSplitSpell{
  def apply(json: JsValue="""{}"""): DataFrameRandomSplitSpell =
    new DataFrameRandomSplitSpell().parseJson(json)
}