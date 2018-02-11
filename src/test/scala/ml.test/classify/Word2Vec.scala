package ml.test.classify

import grimoire.Implicits.jstr2JsValue
import grimoire.ml.classify.transform.{DataFrameLogisticRegressionPredictSpell, DataFrameLogisticRegressionTrainSpell}
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.classify.source.LogisticRegressionModelSource
import grimoire.ml.feature.transform.{DataFrameStringIndexerSpell, DataFrameVectorAssemblerSpell}
import grimoire.ml.target.ModelTarget
import grimoire.spark.target.DataFrameTarget

object Word2Vec extends App{
  globalSpark = SparkSession.
    builder().
    master("local[4]").
    appName("lr").
    enableHiveSupport().
    getOrCreate()

  HiveSource("""{"InputHiveTable":"Word2Vec"}""").
    cast(DataFrameStringIndexerSpell("""{"InputCol":"label","OutputCol":"intlabel"}""")).
    cast(DataFrameTarget("""{"OutputHiveTable":"BucketizerConjure"}""")).
    conjure

}
