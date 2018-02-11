package grimoire.ml.FPMining


import grimoire.ml.configuration.param.{HasMaxLocalProjDBSize, HasMaxPatternLength, HasMinSupport, HasNumPartitions}
import grimoire.transform.Spell
import org.apache.spark.mllib.fpm.{PrefixSpan, PrefixSpanModel}
import org.apache.spark.rdd.RDD
import play.api.libs.json.JsValue
import grimoire.Implicits._
/**
  * Created by sjc505 on 17-6-30.
  */
class PrefixSpanModelSpell extends Spell[RDD[Array[Array[Int]]],PrefixSpanModel[Int]]
  with HasMinSupport with HasMaxPatternLength with HasMaxLocalProjDBSize{

  val prefixSpan = new PrefixSpan()


  override def setup(dat:RDD[Array[Array[Int]]]): Boolean = {
    prefixSpan
      .setMinSupport($(minSupport))
      .setMaxPatternLength($(maxPatternLength))
      .setMaxLocalProjDBSize($(maxLocalProjDBSize))
    super.setup(dat)
  }

  override def transformImpl(dat: RDD[Array[Array[Int]]]): PrefixSpanModel[Int] = {
    prefixSpan.run(dat)
  }
}

object PrefixSpanModelSpell{
  def apply(json: JsValue="""{}"""): PrefixSpanModelSpell =
    new PrefixSpanModelSpell().parseJson(json)
}

