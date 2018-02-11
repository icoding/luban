package grimoire.spark.source.dataframe

import grimoire.configuration.param.{HasInputHiveLabels, HasInputHiveTable}
import grimoire.source.SourceFromFile
import grimoire.spark.globalSpark
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._

class HiveSource extends SourceFromFile[DataFrame] with HasInputHiveTable with HasInputHiveLabels{
  override def conjureImpl: DataFrame = {
    globalSpark.sql("select "+$(inputHiveLabels)+" from " + $(inputHiveTable))
  }
}

object HiveSource extends HiveSource{
  def apply(json: JsValue="""{}"""): HiveSource = new HiveSource().parseJson(json)
}