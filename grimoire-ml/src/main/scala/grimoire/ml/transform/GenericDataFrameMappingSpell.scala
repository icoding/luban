package grimoire.ml.transform

import grimoire.configuration.param.{HasInputCol, HasOriginColumns, HasOutputCol}
import grimoire.dataset.&
import grimoire.Implicits._
import grimoire.spark.globalSpark
import grimoire.spark.transform.dataframe.DataFrameUDFSpell
import grimoire.util.keeper.MapKeeper
import grimoire.transform.Spell
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

import scala.reflect.runtime.universe._


/**
  * Created by caphael on 2017/3/28.
  */
class GenericDataFrameMappingSpell[F:TypeTag,T:TypeTag] extends Spell[DataFrame & MapKeeper[F,T],DataFrame]
  with HasInputCol
  with HasOutputCol
  with HasOriginColumns{

  val dfSpell = new DataFrameUDFSpell

  override def transformImpl(dat: DataFrame & MapKeeper[F, T]): DataFrame = {
    val keeper = globalSpark.sparkContext.broadcast(dat._2)
    dfSpell.setUDF(
      (o:F)=>
        keeper.value.get(o)
    )

    dfSpell.setInputCol($(inputCol)).setOutputCol($(outputCol)).transform(dat._1)
  }
}

object GenericDataFrameMappingSpell{
  def apply[F : TypeTag, T : TypeTag](json: JsValue ="""{}"""): GenericDataFrameMappingSpell[F,T] =
    new GenericDataFrameMappingSpell[F,T]().parseJson(json)
}