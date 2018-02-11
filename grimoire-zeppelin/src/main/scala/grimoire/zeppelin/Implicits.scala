package grimoire.zeppelin

import grimoire.ml.evaluate.result.BinaryClassificationMetricsResult
import grimoire.ml.linalg.LabeledMatrix
import grimoire.zeppelin.script.{BinaryClassificationMetricsOps, DataFrameScriptOps, LabeledMatrixScriptOps}
import org.apache.spark.sql.DataFrame

/**
  * Created by caphael on 2017/7/25.
  */
object Implicits {
  implicit def toScriptOps(df:DataFrame) = DataFrameScriptOps(df)
  implicit def toScriptOps(mat: LabeledMatrix) = LabeledMatrixScriptOps(mat)
  implicit def toScriptOps(res:BinaryClassificationMetricsResult) = BinaryClassificationMetricsOps(res)
//  def toScriptOps(df:DataFrame) = DataFrameScriptOps(df)
}
