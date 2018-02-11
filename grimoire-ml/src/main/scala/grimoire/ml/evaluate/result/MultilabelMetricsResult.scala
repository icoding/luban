package grimoire.ml.evaluate.result

/**
  * Created by sjc505 on 17-7-19.
  */
case class MultilabelMetricsResult ( accuracy: Double ,
                                     subsetAccuracy: Double,
                                     f1Measure: Double ,
                                     precision: Double,
                                     recall: Double,
                                     hammingLoss: Double ,
                                     labels: Array[Double],
                                     microF1Measure: Double ,
                                     microPrecision: Double ,
                                     microRecall: Double
                                   )
