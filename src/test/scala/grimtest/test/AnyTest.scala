package grimtest.test

import grimoire.spark.transform.rdd.StringSeqRDDToDFSpell

/**
  * Created by caphael on 2017/1/6.
  */
object AnyTest extends App{
  import org.apache.spark.sql.SparkSession
  import grimoire.ml.feature.transform.DataFrameTFVectorizeSpell
  import grimoire.nlp.transform.DataFrameSegmentSpell
  import grimoire.ml.feature.transform.IDFTrainSpell
  import grimoire.ml.target.ModelTarget
  import grimoire.ml.feature.transform.DataFrameTFIDFVectorizeSpell
  import grimoire.spark.target.DataFrameTarget
  import grimoire.Implicits._
  import grimoire.ml.feature.source.IDFModelSource
  import grimoire.spark.source.rdd.TextFileRDDSource
  import grimoire.spark.globalSpark
  import grimoire.spark.transform.rdd.RDDLineSplitSpell

  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val vectorize2TF =
    TextFileRDDSource("""{"InputPath":"data/test.txt"}""").
      cast(RDDLineSplitSpell("""{"Separator":"\\s+","NumField":2}""")).
      cast(StringSeqRDDToDFSpell("""{"Schema":"id int,content string"}""")).
      cast(DataFrameSegmentSpell("""{"InputCol":"content","OutputCol":"segmented"}""")).
      cast(DataFrameTFVectorizeSpell("""{"InputCol":"segmented","OutputCol":"tf"}"""))

  val trainIDF = vectorize2TF.cast(IDFTrainSpell("""{"InputCol":"tf"}""")).cast(ModelTarget("""{"OutputPath":"model/idf2","Overwrite":true}"""))
  trainIDF.conjureImpl

  val vectorize2TFIDF =
    (vectorize2TF :+ IDFModelSource("""{"InputPath":"model/idf2"}""")).
    cast(DataFrameTFIDFVectorizeSpell("""{"InputCol":"tf", "OutputCol":"tfidf"}""")).
    cast(DataFrameTarget("""{"OutputPath":"data/tfidf","WriteMethod":"PARQUET"}"""))

  val vectorize2TFCombo =  RDDLineSplitSpell("""{"Separator":"\\s+","NumField":2}""") ~ StringSeqRDDToDFSpell("""{"Schema":"id int,content string"}""") ~ DataFrameSegmentSpell("""{"InputCol":"content","OutputCol":"segmented"}""") ~ DataFrameTFVectorizeSpell("""{"InputCol":"segmented","OutputCol":"tf"}""")
  TextFileRDDSource("""{"InputPath":"data/test.txt"}""").cast(vectorize2TFCombo)

  vectorize2TFIDF.conjure
}
