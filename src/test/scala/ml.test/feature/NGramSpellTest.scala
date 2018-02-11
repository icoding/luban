package ml.test.feature

import grimoire.ml.feature.transform.DataFrameRegexTokenizerSpell


/**
  * Created by Arno on 2017/10/17.
  */
object NGramSpellTest extends App{
  import grimoire.spark.transform.rdd.StringSeqRDDToDFSpell
  import org.apache.spark.sql.SparkSession
  import grimoire.Implicits._
  import grimoire.spark.source.rdd.TextFileRDDSource
  import grimoire.spark.globalSpark
  import grimoire.spark.transform.rdd.RDDLineSplitSpell
  import grimoire.ml.feature.transform.DataFrameNGramSpell


  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()

  val vectorize2seg =
    TextFileRDDSource("""{"InputPath":"data/mllib/MLtest.txt"}""").
      cast(RDDLineSplitSpell("""{"Separator":"\\s+","NumField":2}""")).
      cast(StringSeqRDDToDFSpell("""{"Schema":"lable double,sentence string"}""")).
      cast(DataFrameRegexTokenizerSpell().setInputCol("sentence").setOutputCol("regexsentenct").setPattern("\\W")).
      cast(DataFrameNGramSpell().setInputCol("regexsentenct").setOutputCol("ngramcontent").setN(3))
  vectorize2seg.conjure.show(false)
}
