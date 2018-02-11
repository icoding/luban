package grimoire.ml.parsing.transform

import grimoire.transform.Spell
import org.apache.spark.rdd.RDD
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-7-12.
  */
class RDDDoubleSeqParsingSpell extends Spell[RDD[String],RDD[Seq[Double]]]{
  val single = new DoubleSeqParsingSpell

  override def transformImpl(dat: RDD[String]): RDD[Seq[Double]] = {
    dat.map(single.transformImpl(_))
  }

  override def parseJson(json: JsValue): RDDDoubleSeqParsingSpell.this.type = {
    single.parseJson(json)
    super.parseJson(json)
  }
}
object RDDDoubleSeqParsingSpell{
  def apply(json:JsValue="""{}"""): RDDDoubleSeqParsingSpell =
    new RDDDoubleSeqParsingSpell().parseJson(json)
}

