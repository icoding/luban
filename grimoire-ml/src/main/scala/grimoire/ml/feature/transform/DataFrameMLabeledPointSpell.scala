package grimoire.ml.feature.transform

/**
  * Created by jax on 17-6-29.
  */


import grimoire.ml.configuration.param.{HasFeatureCols, HasLabelCol}
import grimoire.ml.transform.{GenericDataFrameMappingGenSpell, GenericDataFrameMappingSpell}
import grimoire.util.keeper.MapKeeper
import grimoire.Implicits._
import grimoire.dataset.&
import grimoire.spark.transform.dataframe.DataFrameDropColumnSpell
import grimoire.transform.{HasSingleSpellLike, Spell}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import play.api.libs.json.JsValue


private[grimoire] class DataFrameMLabeledPointSpell
  extends Spell[DataFrame & MapKeeper[String,Long],RDD[LabeledPoint]]
    with HasSingleSpellLike[Row,LabeledPoint]
    with HasFeatureCols
    with HasLabelCol{

  def doubleLabelCol:String = $(labelCol) + "$D"

  override def transformImpl(dat: DataFrame & MapKeeper[String, Long]): RDD[LabeledPoint] = {
    val labelMapping:MapKeeper[String,Long] = if (dat._2 == null){
      val labidx = new GenericDataFrameMappingGenSpell[String]().setInputCol($(labelCol))
      labidx.transform(dat._1)
    } else{
      dat._2
    }

    val labelMappingSpell = new GenericDataFrameMappingSpell[String,Long].setOriginColumns(true).setInputCol($(labelCol)).setOutputCol(doubleLabelCol)

    val dropSpell = new DataFrameDropColumnSpell().setDropColNames(Seq($(labelCol)))

    single.setLabelCol(doubleLabelCol)

    //Transform string labels into double
    (labelMappingSpell ~ dropSpell).transform(dat).rdd.map{
      case r =>
        single.transform(r)
    }
  }
  override val single: RowToLabeledPointSpell = new RowToLabeledPointSpell
}

object DataFrameMLabeledPointSpell{
  def apply(json: JsValue): DataFrameMLabeledPointSpell = new DataFrameMLabeledPointSpell().parseJson(json)
}
