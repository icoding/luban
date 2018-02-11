package grimoire.spark.transform.dataframe

import grimoire.configuration.param.HasInputCol
import grimoire.transform.Spell
import grimoire.Implicits.jstr2JsValue
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

import scala.reflect.runtime.universe._
import scala.reflect.ClassTag
/**
  * Created by caphael on 2017/7/17.
  */
class DataFrameSelectToRDDSpell[T:TypeTag](implicit clazzT:ClassTag[T]) extends Spell[DataFrame,RDD[T]] with HasInputCol{
  override def transformImpl(dat: DataFrame): RDD[T] = {
    dat.select($(inputCol)).rdd.map{
      case row =>
        row.get(0).asInstanceOf[T]
    }
  }
}

object DataFrameSelectToRDDSpell{
  def apply[T:TypeTag](json: JsValue = """{}""")(implicit clazzT:ClassTag[T]): DataFrameSelectToRDDSpell[T] = new DataFrameSelectToRDDSpell[T]().parseJson(json)
}
