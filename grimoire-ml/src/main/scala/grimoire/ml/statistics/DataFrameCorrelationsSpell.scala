package grimoire.ml.statistics

import grimoire.configuration.param.{HasCache, HasInputCols}
import grimoire.ml.configuration.param.HasCorrelationMethod
import grimoire.ml.feature.transform.DataFrameVectorAssemblerSpell
import grimoire.ml.linalg.{LabeledDenseMatrix, LabeledMatrix, LabeledSparseMatrix}
import grimoire.transform.Spell
import org.apache.spark.sql.DataFrame
import grimoire.Implicits._
import grimoire.spark.transform.dataframe.DataFrameSelectToRDDSpell
import org.apache.spark.ml.linalg.{DenseMatrix, SparseMatrix, Vector}
import play.api.libs.json._

/**
  * Created by Arno on 2017/11/16.
  */
class DataFrameCorrelationsSpell extends Spell[DataFrame,JsObject] with HasInputCols with HasCorrelationMethod
  with HasCache {
  val vecs = DataFrameVectorAssemblerSpell()
  val df2rdd = DataFrameSelectToRDDSpell[Vector]()
  val corrs = CorrelationsSpell()
  final val tmpOutputCol = "vector4corr"

  override def setup(dat: DataFrame): Boolean = {
    vecs.setInputCols($(inputCols)).setOutputCol(tmpOutputCol)
    df2rdd.setInputCol(tmpOutputCol)
    corrs
      .setCorrelationMethod($(corMethod))
      .setCache($(cache))

    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): JsObject = {
    val labelMatrix = vecs ~ df2rdd ~ corrs transform dat asML match {
      case m:DenseMatrix => LabeledDenseMatrix(m,$(inputCols).toArray,$(inputCols).toArray)
      case m:SparseMatrix => LabeledSparseMatrix(m,$(inputCols).toArray,$(inputCols).toArray)
    }
    val content = labelMatrix.getCols().toList.map{
      _.toArray.map(_.formatted("%.3f").toDouble)
    }
    val outside = labelMatrix.colLabels; val inside = labelMatrix.rowLabels
    val temp:List[JsValue] = content.map(_.map(JsNumber(_).asInstanceOf[JsValue])).map(inside zip _).map(JsObject(_))
    JsObject(outside zip temp)
  }
}

object DataFrameCorrelationsSpell{
  def apply(json: JsValue= """{}"""): DataFrameCorrelationsSpell = new DataFrameCorrelationsSpell().parseJson(json)
}