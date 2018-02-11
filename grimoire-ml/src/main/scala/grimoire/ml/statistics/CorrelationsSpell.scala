package grimoire.ml.statistics

import org.apache.spark.mllib.linalg._
import grimoire.transform.Spell
import org.apache.spark.ml.linalg.Vector
import grimoire.ml.Implicits._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.ml.configuration.param.HasCorrelationMethod

/**
  * Created by sjc505 on 17-7-12.
  */
class CorrelationsSpell extends Spell[RDD[Vector],Matrix] with HasCorrelationMethod{
  override def setup(dat: RDD[Vector]): Boolean = {
    super.setup(dat)
  }

  override def transformImpl(dat: RDD[Vector]): Matrix = {
    Statistics.corr(dat, "pearson")
  }
}

object CorrelationsSpell{
  def apply(json: JsValue="""{}"""): CorrelationsSpell =
    new CorrelationsSpell().parseJson(json)
}
