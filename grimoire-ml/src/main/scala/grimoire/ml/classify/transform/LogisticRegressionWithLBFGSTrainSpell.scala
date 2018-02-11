package grimoire.ml.classify.transform

/**
  * Created by jax on 17-6-29.
  */
import grimoire.dataset.&
import grimoire.ml.Implicits._
import grimoire.ml.configuration.param._
import grimoire.ml.feature.transform.DataFrameMLLabeledPointSpell
import grimoire.transform.Spell
import grimoire.util.keeper.MapKeeper
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}


class LogisticRegressionWithLBFGSTrainSpell
  extends Spell[DataFrame & MapKeeper[String,Long],LogisticRegressionModel]
    with HasNumClasses{
  val labpntSpell = new DataFrameMLLabeledPointSpell
  val lbfgs = new LogisticRegressionWithLBFGS()
  override def parseJson(json: JsValue): LogisticRegressionWithLBFGSTrainSpell.this.type = {
    labpntSpell.parseJson(json)
    super.parseJson(json)
  }

  override def transformImpl(dat: DataFrame & MapKeeper[String, Long]): LogisticRegressionModel = {
    val labpnt = labpntSpell.transform(dat)
    lbfgs.
      setNumClasses($(numClasses)).
      run(labpnt)
  }
}

object LogisticRegressionWithLBFGSTrainSpell{
  def apply(json: JsValue): LogisticRegressionWithLBFGSTrainSpell = new LogisticRegressionWithLBFGSTrainSpell().parseJson(json)
}