package grimoire.spark.source.dataframe

import grimoire.configuration.param.{HasSchema, HasSeparator}
import grimoire.source.SourceFromFile
import grimoire.Implicits._
import grimoire.spark.globalSpark
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

/**
  * Created by caphael on 2017/3/27.
  */
class CSVSource extends SourceFromFile[DataFrame] with HasSchema with HasSeparator{
//  override type ValueType = DataFrame
  separator.default(",")

  override def conjureImpl: DataFrame = {
    globalSpark.read.schema(${schema}).option("sep",$(separator)).csv($(inputPath))
  }
}

object CSVSource extends CSVSource{
  def apply(json: JsValue="""{}"""): CSVSource = new CSVSource().parseJson(json)
}