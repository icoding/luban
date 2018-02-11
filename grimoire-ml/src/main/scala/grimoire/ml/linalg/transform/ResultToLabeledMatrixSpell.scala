package grimoire.ml.linalg.transform

import grimoire.ml.linalg.{LabeledDenseMatrix, LabeledMatrix}
import grimoire.ml.statistics.DataFrameMultivariateStatisticalSummarySpell
import grimoire.ml.statistics.result.MultivariateStatisticalSummaryResult
import grimoire.transform.Spell
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.ml.configuration.param._
import grimoire.ml.parsing.transform.LabeledMatrixUtil


class ResultToLabeledMatrixSpell extends Spell[MultivariateStatisticalSummaryResult,LabeledMatrix]
  with LabeledMatrixUtil with HasNumRows with HasNumCols
  with HasTransposed with HasRowLabels with HasColLabels{
  val sum = new DataFrameMultivariateStatisticalSummarySpell()

  def apply(numRows: Int,
            numCols: Int,
            values: Array[Double],
            isTransposed: Boolean,
            rowLabels: Array[String],
            colLabels: Array[String]
           ): LabeledMatrix = new LabeledDenseMatrix(numRows, numCols, values, isTransposed, rowLabels,colLabels)

  override def transformImpl(dat: MultivariateStatisticalSummaryResult): LabeledMatrix = {
    val count =dat.count
    val s= Array(dat.mean,dat.max,dat.min,dat.normL1,dat.normL2,dat.numNonzeros,dat.variance)
       fromVectors(apply _)(s,$(rowLabels).toArray,$(colLabels).toArray,$(transposed))
  }
}

object ResultToLabeledMatrixSpell{
  def apply(json: JsValue="""{}"""): ResultToLabeledMatrixSpell =
    new ResultToLabeledMatrixSpell().parseJson(json)
}
