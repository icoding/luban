package grimoire.ml.FPMining

import grimoire.ml.configuration.param.{HasMinConfidence, HasMinSupport, HasNumPartitions}
import grimoire.transform.Spell
import org.apache.spark.mllib.fpm.{AssociationRules, FPGrowth, FPGrowthModel}
import org.apache.spark.rdd.RDD
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-6-30.
  */
class AssociationRulesSpell
  extends Spell[RDD[FPGrowth.FreqItemset[String]],RDD[AssociationRules.Rule[String]]]
  with HasMinConfidence {

  val ar = new AssociationRules()

  override protected def setup(dat: RDD[FPGrowth.FreqItemset[String]]): Boolean = {
    ar
      .setMinConfidence($(minConfidence))

    super.setup(dat)
  }

  override def transformImpl(dat:RDD[FPGrowth.FreqItemset[String]]):  RDD[AssociationRules.Rule[String]]= {
    ar.run(dat)
  }

}

object AssociationRulesSpell{
  def apply(json: JsValue="""{}"""): AssociationRulesSpell =
    new AssociationRulesSpell().parseJson(json)
}
