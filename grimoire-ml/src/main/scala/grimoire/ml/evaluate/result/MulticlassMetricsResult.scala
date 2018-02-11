package grimoire.ml.evaluate.result

import org.apache.spark.mllib.linalg.Matrix
import org.dmg.pmml.ConfusionMatrix

/**
  * Created by sjc505 on 17-7-19.
  */
case class MulticlassMetricsResult(confusionMatrix: Matrix,
                                   accuracy: Double,
                                   weightedFMeasure: Double ,
                                   weightedPrecision: Double,
                                   weightedRecall: Double,
                                   weightedFalsePositiveRate: Double,
                                   weightedTruePositiveRate: Double)
