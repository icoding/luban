package grimoire.ml.FPMining

import grimoire.transform.Spell
import org.apache.spark.mllib.fpm.{PrefixSpan, PrefixSpanModel}
import org.apache.spark.rdd.RDD
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.dataset.&
import grimoire.ml.configuration.param.{HasMaxPatternLength, HasMinSupport}

/**
  * Created by sjc505 on 17-6-30.
  */
class PrefixSpanSpell
  extends Spell[RDD[Array[Array[Int]]] & PrefixSpanModel[Int],RDD[PrefixSpan.FreqSequence[Int]]] {
  val prefixSpan = new PrefixSpan()

    override def transformImpl(dat:RDD[Array[Array[Int]]] & PrefixSpanModel[Int]): RDD[PrefixSpan.FreqSequence[Int]] = {
      prefixSpan.run(dat._1)
      dat._2.freqSequences
    }
}

object PrefixSpanSpell{
  def apply(json: JsValue="""{}"""): PrefixSpanSpell =
    new PrefixSpanSpell().parseJson(json)
}

