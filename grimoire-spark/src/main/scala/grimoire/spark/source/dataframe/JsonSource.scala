package grimoire.spark.source.dataframe

import grimoire.configuration.param.HasInputPath
import grimoire.source.SourceFromFile
import grimoire.spark.globalSpark
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by jax on 17-6-28.
  */
class JsonSource extends SourceFromFile[DataFrame] with HasInputPath{
  override def conjureImpl: DataFrame = {
    globalSpark.read.format("json").load($(inputPath))
  }
}

object JsonSource{
  def apply(json:JsValue="""{}"""): JsonSource = {
    new JsonSource().parseJson(json)
  }
}