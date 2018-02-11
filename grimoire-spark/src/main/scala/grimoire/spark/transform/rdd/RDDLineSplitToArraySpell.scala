package grimoire.spark.transform.rdd

import grimoire.common.transform.{LineSplitLike, LineSplitSpell}
import grimoire.configuration.param.HasSchema
import grimoire.transform.{HasSingleSpellLike, Spell}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.StructType
import play.api.libs.json.JsValue

/**
  * Created by sjc505 on 17-6-30.
  */
class RDDLineSplitToArraySpell extends LineSplitLike[RDD[String],RDD[Array[String]]] with HasSingleSpellLike[String,Seq[String]]{
  override val single = new LineSplitSpell()

  override def transformImpl(dat: RDD[String]): RDD[Array[String]] = {
    dat.map(single.transformImpl(_).toArray)
  }

  override def parseJson(json: JsValue): RDDLineSplitToArraySpell.this.type = {
    single.parseJson(json)
    super.parseJson(json)
  }

}

object RDDLineSplitToArraySpell{
  def apply(json:JsValue): RDDLineSplitToArraySpell =
    new RDDLineSplitToArraySpell().parseJson(json)
}

//
//  extends Spell[RDD[Seq[String]],RDD[Array[String]]] with HasSchema{
//
//  override def transformImpl(dat: RDD[Seq[String]]): RDD[Array[String]] = {
//    dat.map()
//  }
//
//}
//
//object StringSeqToArraySpell {
//  def apply(json: JsValue): StringSeqToArraySpell = {
//    new StringSeqToArraySpell().parseJson(json)
//  }
//}