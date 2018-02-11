package grimoire.zeppelin.target

import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.ml.evaluate.result.MulticlassMetricsResult
import grimoire.ml.linalg.LabeledMatrix
import grimoire.target.Target

/**
  * Created by sjc505 on 17-7-26.
  */
class ZeppelinEvaluateMulticlassTarget  extends Target[MulticlassMetricsResult] {

  override def transformImpl(dat: MulticlassMetricsResult): Unit = {
    print(dat.copy())
  }


}
object ZeppelinEvaluateMulticlassTarget{
  def apply(json:JsValue="""{}"""): ZeppelinEvaluateMulticlassTarget =
    new ZeppelinEvaluateMulticlassTarget().parseJson(json)
}
