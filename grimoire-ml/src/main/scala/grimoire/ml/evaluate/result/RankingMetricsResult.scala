package grimoire.ml.evaluate.result

/**
  * Created by sjc505 on 17-7-19.
  */
case class RankingMetricsResult( meanAveragePrecision: Double ,
                                 precisionAt: Double ,
                                 ndcgAt: Double)
