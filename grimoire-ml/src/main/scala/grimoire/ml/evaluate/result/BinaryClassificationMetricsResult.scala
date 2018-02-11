package grimoire.ml.evaluate.result

import org.apache.spark.rdd.RDD

/**
  * Created by sjc505 on 17-7-26.
  */
case class BinaryClassificationMetricsResult (

                                          fMeasureByThreshold:RDD[(Double,Double)],
                                          precisionByThreshold:RDD[(Double,Double)],
                                          recallByThreshold:RDD[(Double,Double)],
                                          roc:RDD[(Double,Double)],
                                          areaUnderPR:Double,
                                          areaUnderROC:Double,
                                          thresholds:RDD[Double]

                                        )
