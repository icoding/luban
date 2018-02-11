package grimoire.ml.FPMining


import org.apache.spark.mllib.fpm.{FPGrowth, FPGrowthModel}
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.ml.configuration.param.{HasMinSupport, HasNumPartitions}
import grimoire.transform.Spell
import org.apache.spark.rdd.RDD

/**
  * Created by sjc505 on 17-6-29.
  */
class FPGrowthModelSpell
  extends Spell[RDD[Array[String]],FPGrowthModel[String]]
    with HasMinSupport with HasNumPartitions {

  val fpg = new FPGrowth()

  override def setup(dat:RDD[Array[String]]): Boolean = {
    fpg
      .setMinSupport($(minSupport))
      .setNumPartitions($(numPartitions))

    super.setup(dat)
  }

  override def transformImpl(dat: RDD[Array[String]]): FPGrowthModel[String] = {
   fpg.run(dat)
  }

}

object FPGrowthModelSpell{
  def apply(json: JsValue="""{}"""): FPGrowthModelSpell =
    new FPGrowthModelSpell().parseJson(json)
}
