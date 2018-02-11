package grimoire.ml.evaluate.dataset

import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Matrix


/**
  * Created by caphael on 2017/7/14.
  */
case class MultiClassMetricsSet(
                               labels:Array[Double],
                               confusionMatrix: Matrix,
                               accuracy:Double,
                               precisions:Map[Double,Double],
                               recalls:Map[Double,Double]
                               )

object MultiClassMetricsSet{
  def apply(metrics:MulticlassMetrics): MultiClassMetricsSet =
    new MultiClassMetricsSet(
      metrics.labels,
      metrics.confusionMatrix,
      metrics.accuracy,
      metrics.labels.map{
        case l => l -> metrics.precision(l)
      }.toMap,
      metrics.labels.map{
        case l => l -> metrics.recall(l)
      }.toMap
    )
}
