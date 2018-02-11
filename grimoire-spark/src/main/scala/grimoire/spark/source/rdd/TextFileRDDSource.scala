package grimoire.spark.source.rdd

import grimoire.configuration.param.HasInputPath
import grimoire.source.SourceFromFile
import grimoire.spark.globalSpark
import org.apache.spark.rdd.RDD
import play.api.libs.json.JsValue
/**
  * Created by caphael on 2016/12/12.
  */
class TextFileRDDSource extends SourceFromFile[RDD[String]] with HasInputPath{
  override def conjureImpl: RDD[String] = {
    globalSpark.sparkContext.textFile($(inputPath))
  }
}

object TextFileRDDSource {
  def apply(json:JsValue): TextFileRDDSource = {
    new TextFileRDDSource().parseJson(json)
  }
}


