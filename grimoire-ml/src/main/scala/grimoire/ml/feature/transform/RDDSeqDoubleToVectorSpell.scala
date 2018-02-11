package grimoire.ml.feature.transform

import grimoire.transform.Spell
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.spark.transform.rdd.RDDSpell

/**
  * Created by sjc505 on 17-7-12.
  */
class RDDSeqDoubleToVectorSpell extends RDDSpell[Seq[Double],Vector] {

  override val single: Spell[Seq[Double], Vector] =  new SeqDoubleToVectorSpell

  override def transformImpl(dat: RDD[Seq[Double]]): RDD[Vector] = {
    dat.map(single.transformImpl(_))
  }
   override def parseJson(json: JsValue): RDDSeqDoubleToVectorSpell.this.type = {
    single.parseJson(json)
    super.parseJson(json)
  }
}

object RDDSeqDoubleToVectorSpell{
  def apply(json:JsValue="""{}"""): RDDSeqDoubleToVectorSpell =
    new RDDSeqDoubleToVectorSpell().parseJson(json)

}
