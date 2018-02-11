package grimoire.ml.evaluate.result

/**
  * Created by sjc505 on 17-7-19.
  */
case class RegressionMetricsResult( explainedVariance: Double ,
                                    meanAbsoluteError: Double ,
                                    meanSquaredError: Double ,
                                    r2: Double ,
                                    rootMeanSquaredError: Double )
