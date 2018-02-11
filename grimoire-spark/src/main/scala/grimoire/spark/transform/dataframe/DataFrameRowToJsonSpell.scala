package grimoire.spark.transform.dataframe

import grimoire.configuration.param.HasOutputCol
import grimoire.spark.transform.single.RowToJsonSpell
import grimoire.transform.{HasSingleSpellLike, Spell}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import play.api.libs.json.JsValue


/**
  * Created by caphael on 2017/3/23.
  */
class DataFrameRowToJsonSpell extends DataFrameSpell with HasSingleSpellLike[Row,String] with HasOutputCol{

  override val single: Spell[Row, String] = new RowToJsonSpell

  val schema = StructType{Seq(StructField($(outputCol),StringType,false))}

  override def transformImpl(dat: DataFrame): DataFrame = {

    val rdd = dat.rdd.map{
      case r =>
        Row(single.transformImpl(r))
    }

    dat.sparkSession.createDataFrame(rdd,schema)

  }

  override def parseJson(json: JsValue): DataFrameRowToJsonSpell.this.type = {
    single.parseJson(json)
    super.parseJson(json)
  }
}

object DataFrameRowToJsonSpell{
  def apply(): DataFrameRowToJsonSpell = new DataFrameRowToJsonSpell()

  def apply(json: JsValue): DataFrameRowToJsonSpell = new DataFrameRowToJsonSpell().parseJson(json)
}
