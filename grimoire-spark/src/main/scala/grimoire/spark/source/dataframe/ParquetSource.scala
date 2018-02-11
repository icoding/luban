package grimoire.spark.source.dataframe

import grimoire.spark.globalSpark
import grimoire.Implicits.jstr2JsValue
import grimoire.source.SourceFromFile
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

/**
  * Created by caphael on 2017/1/9.
  */
class ParquetSource extends SourceFromFile[DataFrame]{
  override def conjureImpl: DataFrame = {
    globalSpark.sqlContext.read.parquet($(inputPath))
  }
}

object ParquetSource{
  def apply(json:JsValue="""{}"""): ParquetSource = new ParquetSource().parseJson(json)
}