package grimoire.spark.source.dataframe

import grimoire.configuration.param.HasSQLQuery
import grimoire.source.Source
import grimoire.spark.globalSpark
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

/**
  * Created by caphael on 2017/1/22.
  */
class SQLQuerySource extends Source[DataFrame] with HasSQLQuery {
  override def conjureImpl: DataFrame = {
    globalSpark.sql($(sqlQuery))
  }
}

object SQLQuerySource {
  def apply(json: JsValue): SQLQuerySource = new SQLQuerySource().parseJson(json)
}