package grimoire.ml.parsing.transform

import grimoire.transform.Spell
import org.apache.spark.rdd.RDD
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-7-3.
  */
class RDDIntArray2DParsingSpell extends Spell[RDD[String],RDD[Array[Array[Int]]]]{
  val single = new IntArray2DParsingSpell

  override def transformImpl(dat: RDD[String]): RDD[Array[Array[Int]]] = {
    dat.map(single.transformImpl(_))
  }

  override def parseJson(json: JsValue): RDDIntArray2DParsingSpell.this.type = {
    single.parseJson(json)
    super.parseJson(json)
  }
}
object RDDIntArray2DParsingSpell{
  def apply(json:JsValue="""{}"""): RDDIntArray2DParsingSpell =
    new RDDIntArray2DParsingSpell().parseJson(json)
}