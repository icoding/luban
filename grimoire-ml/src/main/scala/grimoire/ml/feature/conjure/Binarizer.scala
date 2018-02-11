package grimoire.ml.feature.conjure
/**
  * Created by Aron on 17-11-20.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.ml.feature.transform.DataFrameBinarizerSpell
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._

object Binarizer {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      appName("Binarizer").
      enableHiveSupport().
      getOrCreate()

    HiveSource(args(0)).
      cast(DataFrameBinarizerSpell(args(1))).
      cast(DataFrameTarget(args(2))).
      conjure

//    case class Config(inputHiveSource:String="Binarizer",
//                      inputCol:String="feature",
//                      outputCol:String="binarized_feature",
//                      target:String="BinarizerConjure")
//
//    val parser = new scopt.OptionParser[Config]("scopt") {
//      head("scopt", "3.3")
//      opt[String]('h', "inputHiveSource").action((x, c) =>
//        c.copy(inputHiveSource = x)).text("bar is an string property")
//      opt[String]('i', "inputCol").action((x, c) =>
//        c.copy(inputCol = x)).text("bar is an string property")
//      opt[String]('o', "outputCol").action((x, c) =>
//        c.copy(outputCol = x)).text("bar is an string property")
//      opt[String]('t', "target").action((x, c) =>
//        c.copy(target = x)).text("bar is an string property")
//    }
//
//
//    parser.parse(args, Config()) match {
//      case Some(config) =>
//        HiveSource().setInputHiveTable(config.inputHiveSource).
//          cast(DataFrameBinarizerSpell().setInputCol(config.inputCol).setOutputCol(config.outputCol)).
//          cast(DataFrameTarget().setOutputHiveTable(config.outputCol)).
//          conjure
//
//      case None =>
//      // arguments are bad, error message will have been displayed
//    }

  }
}
