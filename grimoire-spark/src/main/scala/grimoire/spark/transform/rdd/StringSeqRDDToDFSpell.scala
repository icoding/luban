package grimoire.spark.transform.rdd

import grimoire.configuration.param.HasSchema
import grimoire.transform.Spell
import grimoire.Implicits._
import grimoire.spark.globalSpark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import play.api.libs.json.JsValue
/**
  * Created by caphael on 2017/1/9.
  */
class StringSeqRDDToDFSpell extends Spell[RDD[Seq[String]],DataFrame] with HasSchema{

  override def transformImpl(dat: RDD[Seq[String]]): DataFrame = {
    val dataConvertFuncs = $(schema).map{
      case sf=>
        getDataConvert(sf.dataType.typeName)
    }

    val rdd = dat.map{
      case arr =>
        arr.zip(dataConvertFuncs).map{
          case (d,f) => f(d)
        }
    }.map(Row(_:_*))

    globalSpark.createDataFrame(rdd,$(schema))
  }

  def setParam(s:Option[StructType]):this.type ={
    setSchema(s)
  }
}

object StringSeqRDDToDFSpell {

  def apply(json:JsValue="""{}"""): StringSeqRDDToDFSpell = {
    new StringSeqRDDToDFSpell().parseJson(json)
  }
}