package grimoire.spark.source.dataframe

import grimoire.configuration.param.HasInputPath
import grimoire.source.SourceFromFile
import grimoire.spark.globalSpark
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

/**
  * Created by jax on 17-6-27.
  */
class LibSvmSource extends SourceFromFile[DataFrame] with HasInputPath{
  override def conjureImpl: DataFrame = {
    globalSpark.read.format("libsvm").load($(inputPath))
  }
}

object LibSvmSource{
  def apply(json:JsValue): LibSvmSource = {
    new LibSvmSource ().parseJson(json)
  }
}