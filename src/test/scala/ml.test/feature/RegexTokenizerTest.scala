package ml.test.feature

/**
  * Created by Arno on 2017/10/17.
  */

object RegexTokenizerTest {
  import grimoire.spark.transform.rdd.StringSeqRDDToDFSpell
  import org.apache.spark.sql.SparkSession
  import grimoire.Implicits._
  import grimoire.spark.source.rdd.TextFileRDDSource
  import grimoire.spark.globalSpark
  import grimoire.spark.transform.rdd.RDDLineSplitSpell
  import grimoire.ml.feature.transform.DataFrameRegexTokenizerSpell


  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()

  val vectorize2seg =
    TextFileRDDSource("""{"InputPath":"data/mllib/MLtest.txt"}""").
      cast(RDDLineSplitSpell("""{"Separator":"\\s+","NumField":2}""")).
      cast(StringSeqRDDToDFSpell("""{"Schema":"lable double,sentence string"}""")).
      cast(DataFrameRegexTokenizerSpell().setInputCol("sentence").setOutputCol("regexsentenct").setPattern("\\W"))
  vectorize2seg.conjure.show(false)
}
