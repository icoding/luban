package grimoire.ml.statistics


import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.linalg.{Vector, Vectors}
import grimoire.ml.linalg.{LabeledDenseMatrix, LabeledMatrix}
import grimoire.transform.Spell
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.dataset.&
import grimoire.ml.Implicits._
import grimoire.ml.configuration.param._
import grimoire.ml.parsing.transform.LabeledMatrixUtil

/**
  * Created by sjc505 on 17-7-12.
  */
class MultivariateStatisticalSummarySpell extends Spell[RDD[Vector],LabeledMatrix]
  with LabeledMatrixUtil with HasNumRows with HasNumCols
  with HasTransposed with HasRowLabels with HasColLabels{

  override def setup(dat: RDD[Vector]): Boolean = {
    val summary: MultivariateStatisticalSummary = Statistics.colStats(dat)
    super.setup(dat)
  }

  def apply(numRows: Int,
            numCols: Int,
            values: Array[Double],
            isTransposed: Boolean,
            rowLabels: Array[String],
            colLabels: Array[String]
           ): LabeledMatrix = new LabeledDenseMatrix(numRows, numCols, values, isTransposed, rowLabels,colLabels)

  override def transformImpl(dat: RDD[Vector]): LabeledMatrix = {
    val summary: MultivariateStatisticalSummary = Statistics.colStats(dat)
    val count:Double = summary.count
//    val cvec:Vector = Vectors.dense(count,count,count,count)
    val cvec:Vector = Vectors.dense(summary.max.toArray.map(x=>count))
    val s:Array[Vector]= Array(cvec, summary.max,summary.min,
      summary.mean,summary.normL1,summary.normL2,summary.numNonzeros,summary.variance)

    fromVectors(apply _)(s,$(rowLabels).toArray,$(colLabels).toArray,$(transposed))

  }
}

object MultivariateStatisticalSummarySpell{
  def apply(json: JsValue="""{}"""): MultivariateStatisticalSummarySpell =
    new MultivariateStatisticalSummarySpell().parseJson(json)
}