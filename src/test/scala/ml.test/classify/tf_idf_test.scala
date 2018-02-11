package ml.test.classify


import grimoire.Implicits.jstr2JsValue
import grimoire.ml.feature.transform.{DataFrameRegexTokenizerSpell, DataFrameTFIDFVectorizeSpell, DataFrameTFVectorizeSpell, IDFTrainSpell}
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.feature.source.IDFModelSource
import grimoire.ml.target.ModelTarget
import grimoire.spark.target.DataFrameTarget

object tf_idf_test {
  globalSpark = SparkSession.
    builder().
    appName("IDFTrain").
    master("local[1]").
    enableHiveSupport().
    getOrCreate()

  HiveSource("""{"InputHiveTable":"RegexTokenizer"}""").
    cast(DataFrameRegexTokenizerSpell("""{"InputCol":"sentence","OutputCol":"words","Pattern":"\\W"}""")).
    cast(DataFrameTarget("""{"OutputHiveTable":"RegexTokenizerConjure"}""")).
    conjure

  HiveSource("""{"InputHiveTable":"RegexTokenizerConjure"}""").
    cast(DataFrameTFVectorizeSpell("""{"InputCol":"words","OutputCol":"tf","NumFeatures":100,"Binary":true}""")).
    cast(DataFrameTarget("""{"OutputHiveTable":"tfconjure"}""")).
    conjure

  HiveSource("""{"InputHiveTable":"tfconjure"}""").
    cast(IDFTrainSpell("""{"InputCol":"tf", "OutputCol":"tfidf"}""")).
    cast(ModelTarget("""{"OutputModelTarget":"hdfs:///user/model/idf2","Overwrite":true}""")).
    conjure

  (HiveSource("""{"InputHiveTable":"tfconjure"}""") :+ IDFModelSource("""{"InputModelSource":"hdfs:///user/model/idf2"}""")).
    cast(DataFrameTFIDFVectorizeSpell()).
    cast(DataFrameTarget("""{"OutputHiveTable":"TFIDFconjure"}""")).conjure

}
