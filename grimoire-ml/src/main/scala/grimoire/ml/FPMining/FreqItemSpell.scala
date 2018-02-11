package grimoire.ml.FPMining

import grimoire.ml.configuration.param.{HasMinSupport, HasNumPartitions}
import grimoire.transform.Spell
import org.apache.spark.mllib.fpm.{AssociationRules, FPGrowth, FPGrowthModel}
import org.apache.spark.rdd.RDD
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-6-30.
  */
class FreqItemSpell extends Spell[FPGrowthModel[String],RDD[FPGrowth.FreqItemset[String]]]
  with HasMinSupport with HasNumPartitions {

  //  val fpg = new FPGrowth()

    override def transformImpl(dat:FPGrowthModel[String]): RDD[FPGrowth.FreqItemset[String]] = {

      dat.freqItemsets
    }


}

object FreqItemSpell{
  def apply(json: JsValue="""{}"""): FreqItemSpell =
    new FreqItemSpell().parseJson(json)
}

