package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import grimoire.Implicits._
import grimoire.ml.configuration.param.HasMinDocFreq
import grimoire.transform.Spell
import org.apache.spark.ml.feature.{IDF, IDFModel}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

/**
  * Created by Arno on 2017/10/16.
  */
class IDFTrainSpell extends Spell[DataFrame,IDFModel] with HasInputCol with HasMinDocFreq
  with HasOutputCol{

  val idf = new IDF()
  override def setup(dat: DataFrame): Boolean = {
    idf
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
      .setMinDocFreq($(minDocFreq))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): IDFModel = {
    idf.fit(dat)
  }
}

object IDFTrainSpell {

  def apply(json:JsValue="""{}"""): IDFTrainSpell = new IDFTrainSpell().parseJson(json)
}