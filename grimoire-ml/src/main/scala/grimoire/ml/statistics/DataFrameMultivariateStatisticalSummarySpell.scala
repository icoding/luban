package grimoire.ml.statistics


import grimoire.transform.Spell
import org.apache.spark.sql.DataFrame
import play.api.libs.json._
import grimoire.Implicits._
import grimoire.configuration.param.HasCache
import grimoire.ml.configuration.param._
import grimoire.ml.feature.transform.DataFrameVectorAssemblerSpell
import grimoire.spark.transform.dataframe.DataFrameSelectToRDDSpell
import org.apache.spark.ml.linalg.{DenseMatrix, SparseMatrix, Vector}

import scala.language.postfixOps

/**
  * Created by Arno on 17-11-15.
  */
class DataFrameMultivariateStatisticalSummarySpell extends Spell[DataFrame,JsObject]
  with HasInputCols with HasRowLabels with HasColLabels with HasTransposed with HasCache
  with HasNumCols with HasNumRows{
  val vecs = DataFrameVectorAssemblerSpell()
  val df2rdd = DataFrameSelectToRDDSpell[Vector]()
  val sum = MultivariateStatisticalSummarySpell()

  final val tmpOutputCol = "vector4sum"

  override def setup(dat: DataFrame): Boolean = {
    vecs.setInputCols($(inputCols)).setOutputCol(tmpOutputCol)
    df2rdd.setInputCol(tmpOutputCol)
    sum
      .setRowLabels($(rowLabels))
      .setColLabels($(colLabels))
      .setTransposed($(transposed))
      .setCache($(cache))
      .setNumCols($(numCols))
      .setNumRows($(numRows))

    super.setup(dat)
  }
  override def transformImpl(dat: DataFrame): JsObject = {
    val LM = vecs ~ df2rdd ~ sum transform dat
    val content = LM.getCols().toList.map{
      _.toArray.map(_.formatted("%.3f").toDouble)
    }

    val outside = LM.colLabels; val inside = LM.rowLabels
    val temp:List[JsValue] = content.map(_.map(JsNumber(_).asInstanceOf[JsValue])).map(inside zip _).map(JsObject(_))
    JsObject(outside zip temp)
  }
}

object DataFrameMultivariateStatisticalSummarySpell{
  def apply(json: JsValue="""{}"""): DataFrameMultivariateStatisticalSummarySpell =
    new DataFrameMultivariateStatisticalSummarySpell().parseJson(json)
}