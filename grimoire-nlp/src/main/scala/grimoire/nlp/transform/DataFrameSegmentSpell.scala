package grimoire.nlp.transform

import grimoire.configuration.param._
import grimoire.nlp.segment.Segmenter
import grimoire.nlp.segment.hanlp.HanlpSegLike
import grimoire.nlp.segment.term.GTerm
import grimoire.Implicits._
import grimoire.spark.transform.dataframe.DataFrameUDFSpell
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

/**
  * Created by caphael on 2017/1/10.
  */
class DataFrameSegmentSpell extends DataFrameUDFSpell with HasInputCol with HasOutputCol
with HasNature with HasCustomDictPaths with HasStopWordsDictPath {

  val segmenter = new Segmenter with HanlpSegLike

  override protected def setup(dat:DataFrame): Boolean = {
    if($(customDictPaths) != null) segmenter.loadUserDict($(customDictPaths))
    if($(stopWordsDictPath) != null) segmenter.loadStopWordsDict(($(stopWordsDictPath)))

    setUDF {
      (s: String) =>
        val natureSet: Set[String] = $(natures).toSet
        GTerm.natureOut = $(nature)
        if ($(toFilter)) {
          segmenter.segment(s).filter(x => natureSet.contains(x.nature)).map(x => x.toString)
        } else if ($(toDrop)) {
          segmenter.segment(s).filter(x => !natureSet.contains(x.nature)).map(x => x.toString)
        } else {
          segmenter.segment(s).map(x => x.toString)
        }
    }
    true
  }

}

object DataFrameSegmentSpell{
  def apply(json:JsValue="""{}"""):DataFrameSegmentSpell =
    new DataFrameSegmentSpell().parseJson(json)
}
