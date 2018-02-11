package grimoire.ml.statistics.result

import org.apache.spark.ml.linalg.Vector
import grimoire.ml.Implicits._

/**
  * Created by sjc505 on 17-7-19.
  */
case class MultivariateStatisticalSummaryResult (
                                                  count: Long,
                                                  mean: Vector,
                                                  max: Vector,
                                                  min: Vector,
                                                  normL1: Vector,
                                                  normL2: Vector,
                                                  numNonzeros: Vector,
                                                  variance: Vector
                                                )
