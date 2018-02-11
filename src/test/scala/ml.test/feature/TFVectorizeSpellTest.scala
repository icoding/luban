package ml.test.feature

/**
  * Created by Arno on 2017/10/16.
  */

object TFVectorizeSpellTest extends App{
  import grimoire.spark.transform.rdd.StringSeqRDDToDFSpell
  import org.apache.spark.sql.SparkSession
  import grimoire.ml.feature.transform.DataFrameTFVectorizeSpell
  import grimoire.nlp.transform.DataFrameSegmentSpell
  import grimoire.Implicits._
  import grimoire.spark.source.rdd.TextFileRDDSource
  import grimoire.spark.globalSpark
  import grimoire.spark.transform.rdd.RDDLineSplitSpell
  import grimoire.ml.feature.transform.IDFTrainSpell
  import grimoire.ml.feature.transform.DataFrameTFIDFVectorizeSpell

  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()

  val vectorize2TF =
    TextFileRDDSource("""{"InputPath":"data/mllib/MLtest.txt"}""").
      cast(RDDLineSplitSpell("""{"Separator":"\\s+","NumField":2}""")).
      cast(StringSeqRDDToDFSpell("""{"Schema":"lable double,sentence string"}""")).
      cast(DataFrameSegmentSpell("""{"InputCol":"sentence","OutputCol":"segmented"}""")).
      cast(DataFrameTFVectorizeSpell().setInputCol("segmented").setOutputCol("tf").setNumFeatures(100).setBinary(true))

  vectorize2TF.conjure.show(false)

  val vectorize2IdfModel = vectorize2TF.
    cast(IDFTrainSpell().setInputCol("tf"))

  val vectorize2TfIdf = ( vectorize2TF :+ vectorize2IdfModel).
    cast(DataFrameTFIDFVectorizeSpell())

  vectorize2TfIdf.conjure.show(false)
}
