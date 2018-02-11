package grimoire.ml.parsing.transform

import grimoire.transform.Spell
import org.apache.spark.rdd.RDD
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-7-19.
  */
class RDDDouble2DParsingSpell extends Spell[RDD[String],RDD[(Double,Double)]]{
  val single = new Double2DParsingSpell

  override def transformImpl(dat: RDD[String]): RDD[(Double,Double)] = {
    dat.map(single.transformImpl(_))
  }

  override def parseJson(json: JsValue): RDDDouble2DParsingSpell.this.type = {
    single.parseJson(json)
    super.parseJson(json)
  }
}
object RDDDouble2DParsingSpell{
  def apply(json:JsValue="""{}"""): RDDDouble2DParsingSpell =
    new RDDDouble2DParsingSpell().parseJson(json)
}

