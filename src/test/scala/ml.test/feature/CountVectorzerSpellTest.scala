package ml.test.feature

object CountVectorzerSpellTest {
  import grimoire.ml.feature.transform.DataFrameCountVectorzerSpell
  import grimoire.spark.transform.rdd.StringSeqRDDToDFSpell
  import org.apache.spark.sql.SparkSession
  import grimoire.nlp.transform.DataFrameSegmentSpell
  import grimoire.Implicits._
  import grimoire.spark.source.rdd.TextFileRDDSource
  import grimoire.spark.globalSpark
  import grimoire.spark.transform.rdd.RDDLineSplitSpell


  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()

  val vectorize2CV =
    TextFileRDDSource("""{"InputPath":"data/mllib/countvectest.txt"}""").
      cast(RDDLineSplitSpell("""{"Separator":" ","NumField":2}""")).
      cast(StringSeqRDDToDFSpell("""{"Schema":"lable double,sentence string"}""")).
      cast(DataFrameSegmentSpell("""{"InputCol":"sentence","OutputCol":"segmented"}""")).
      cast(DataFrameCountVectorzerSpell().setInputCol("segmented").setOutputCol("countvectorize"))

}
