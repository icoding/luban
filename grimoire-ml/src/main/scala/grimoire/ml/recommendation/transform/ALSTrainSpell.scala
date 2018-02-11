package grimoire.ml.recommendation.transform

import grimoire.Implicits._
import grimoire.configuration.param.{HasMaxIter, HasRegParam}
import grimoire.transform.Spell
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.DataFrame

/**
  * Created by caphael on 2017/2/20.
  */
class ALSTrainSpell extends Spell[DataFrame,ALSModel] with HasMaxIter with HasRegParam{

  override def transformImpl(dat: DataFrame): ALSModel = {
    val als = new ALS().setMaxIter($(maxIter)).setRegParam($(regParam))
    als.fit(dat)
  }
}

object ALSTrainSpell{
  def apply(json: String): ALSTrainSpell =
    new ALSTrainSpell().parseJson(json)
}
