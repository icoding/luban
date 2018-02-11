package grimoire.ml.FPMining

import grimoire.ml.configuration.param.{HasMinSupport, HasNumPartitions}
import grimoire.transform.Spell
import org.apache.spark.mllib.fpm.{AssociationRules, FPGrowth, FPGrowthModel}
import org.apache.spark.rdd.RDD
import org.dmg.pmml.Item
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.dataset.&
import org.apache.spark.mllib.fpm.AssociationRules.Rule

/**
  * Created by sjc505 on 17-6-29.
  */
class FPGrowthSpell
  extends Spell[FPGrowthModel[String],RDD[AssociationRules.Rule[String]]/*Array[FPGrowth.FreqItemset[String]]*/]
  with HasMinSupport with HasNumPartitions {

  override def transformImpl(dat:FPGrowthModel[String]):  RDD[AssociationRules.Rule[String]]= {

     val minConfidence = 0.8
     dat.generateAssociationRules(minConfidence)
  }
}

object FPGrowthSpell{
  def apply(json: JsValue="""{}"""): FPGrowthSpell =
    new FPGrowthSpell().parseJson(json)
}

