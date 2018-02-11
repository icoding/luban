package grimoire.spark.transform.rdd


import grimoire.transform.Spell
import org.apache.spark.rdd.RDD
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.spark.transform.StringToDoubleSpell

/**
  * Created by sjc505 on 17-7-12.
  */
class RDDStringToDoubleSpell extends RDDSpell[String,Double] {

  override val single: Spell[String, Double] =  new StringToDoubleSpell

  override def transformImpl(dat: RDD[String]): RDD[Double] = {
    dat.map(single.transformImpl(_))
  }
  override def parseJson(json: JsValue): RDDStringToDoubleSpell.this.type = {
    single.parseJson(json)
    super.parseJson(json)
  }
}
object RDDStringToDoubleSpell{
  def apply(json:JsValue="""{}"""): RDDStringToDoubleSpell =
    new RDDStringToDoubleSpell().parseJson(json)

}

