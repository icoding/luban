package grimoire.ml.feature.transform

import grimoire.dataset.&
import grimoire.ml.configuration.param._
import grimoire.transform.Spell
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by jax on 17-6-28.
  */
class TakeDFSpell extends Spell[DataFrame&DataFrame,DataFrame] with HasTakeTrain{
  override def transformImpl(dat: &[DataFrame, DataFrame]): DataFrame = {
    if ($(taketrain)) dat._1 else dat._2
  }
}

object TakeDFSpell{
  def apply(json: JsValue="""{}"""): TakeDFSpell =
    new TakeDFSpell().parseJson(json)
}