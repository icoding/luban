package grimoire.spark.transform.rdd

import grimoire.configuration.param.HasSchema
import grimoire.spark.globalSpark
import grimoire.spark.transform.single.JsonToRowSpell
import grimoire.transform.Spell
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

/**
  * Created by caphael on 2017/3/21.
  */
class JsonRDDToDFSpell extends Spell[RDD[String],DataFrame] with HasSchema{
  val single = new JsonToRowSpell

  override def transformImpl(dat: RDD[String]): DataFrame = {
    val rdd = dat.map(single.transformImpl(_))
    globalSpark.createDataFrame(rdd,$(schema))
  }

  override def parseJson(json: JsValue): JsonRDDToDFSpell.this.type = {
    single.parseJson(json)
    super.parseJson(json)
    this
  }

}