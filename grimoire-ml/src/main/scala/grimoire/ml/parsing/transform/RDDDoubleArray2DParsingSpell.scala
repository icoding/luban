package grimoire.ml.parsing.transform

import grimoire.transform.Spell
import org.apache.spark.rdd.RDD
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-7-6.
  */
class RDDDoubleArray2DParsingSpell  extends Spell[RDD[String],RDD[(Array[Double],Array[Double])]]{
  val single = new DoubleArray2DParsingSpell

  override def transformImpl(dat: RDD[String]): RDD[(Array[Double],Array[Double])] = {
    dat.map(single.transformImpl(_))
  }

  override def parseJson(json: JsValue): RDDDoubleArray2DParsingSpell.this.type = {
    single.parseJson(json)
    super.parseJson(json)
  }
}
object RDDDoubleArray2DParsingSpell{
  def apply(json:JsValue="""{}"""): RDDDoubleArray2DParsingSpell =
    new RDDDoubleArray2DParsingSpell().parseJson(json)
}
