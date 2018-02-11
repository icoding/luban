package grimoire.ml.feature.conjure
/**
  * Created by Aron on 17-11-20.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.ml.feature.transform.DataFrameVectorAssemblerSpell
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import play.api.libs.json._

object VectorAssembler {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      appName("VectorAssembler").
      enableHiveSupport().
      getOrCreate()

    HiveSource(args(0)).
      cast(DataFrameVectorAssemblerSpell(
        Json.obj(
          "InputCols"-> (Json.parse(args(1)) \ "InputCols").as[String].split(",").map(_.trim),
          "OutputCol" -> (Json.parse(args(1)) \ "OutputCol").as[String]
        ).toString()
      )).
      cast(DataFrameTarget(args(2))).
      conjure
  }
}
