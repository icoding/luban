package grimoire.spark.transform.rdd

import grimoire.common.transform.{LineSplitLike, LineSplitSpell}
import grimoire.transform.HasSingleSpellLike
import org.apache.spark.rdd.RDD
import play.api.libs.json.JsValue


/**
  * Created by sjc505 on 17-7-12.
  */
class RDDLineSplitToVectorSpell extends LineSplitLike[RDD[String],RDD[Seq[String]]] with HasSingleSpellLike[String,Seq[String]]{
  override val single = new LineSplitSpell()

  override def transformImpl(dat: RDD[String]): RDD[Seq[String]] = {
    dat.map(single.transform(_).toSeq)
  }
  override def parseJson(json: JsValue): RDDLineSplitToVectorSpell.this.type = {
    single.parseJson(json)
    super.parseJson(json)
  }
}

object RDDLineSplitToVectorSpell{
  def apply(json:JsValue): RDDLineSplitToVectorSpell =
    new RDDLineSplitToVectorSpell().parseJson(json)
}
