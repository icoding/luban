package grimoire.spark.transform.rdd

import grimoire.common.transform.{LineSplitLike, LineSplitSpell}
import grimoire.transform.HasSingleSpellLike
import grimoire.Implicits._
import org.apache.spark.rdd.RDD
import play.api.libs.json.JsValue


/**
  * Created by caphael on 2016/12/12.
  */
class RDDLineSplitSpell extends LineSplitLike[RDD[String],RDD[Seq[String]]] with HasSingleSpellLike[String,Seq[String]]{
  override val single = new LineSplitSpell

  override def transformImpl(dat: RDD[String]): RDD[Seq[String]] = {
    dat.map(single.transformImpl(_))
  }

  override def parseJson(json: JsValue): RDDLineSplitSpell.this.type = {
    single.parseJson(json)
    super.parseJson(json)
  }

}

object RDDLineSplitSpell{
  def apply(json:JsValue="""{}"""): RDDLineSplitSpell =
    new RDDLineSplitSpell().parseJson(json)
}