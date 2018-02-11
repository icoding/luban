package grimtest.ml

/**
  * Created by caphael on 2017/3/22.
  */
object LDATest {
  import org.apache.spark.sql.SparkSession
  import grimoire.ml.feature.transform.DataFrameTFVectorizeSpell
  import grimoire.nlp.transform.DataFrameSegmentSpell
  import grimoire.spark.transform.rdd.StringSeqRDDToDFSpell
  import grimoire.ml.feature.transform.IDFTrainSpell
  import grimoire.ml.target.ModelTarget
  import grimoire.ml.feature.transform.DataFrameTFIDFVectorizeSpell
  import grimoire.spark.target.DataFrameTarget
  import grimoire.Implicits._
  import grimoire.ml.clustering.transform.LDATrainSpell
  import grimoire.ml.feature.source.IDFModelSource
  import grimoire.spark.globalSpark
  import grimoire.spark.transform.rdd.RDDLineSplitSpell
  import grimoire.ml.clustering.source.LDAModelSource
  import grimoire.ml.clustering.transform.LDATransformSpell
  import grimoire.spark.source.rdd.TextFileRDDSource
  import grimoire.spark.transform.dataframe.DataFrameSelectSpell


  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val vectorize2TF =
    TextFileRDDSource("""{"InputPath":"data/test.txt"}""").
      cast(RDDLineSplitSpell("""{"Separator":"\\s+","NumField":2}""")).
      cast(StringSeqRDDToDFSpell("""{"Schema":"id int,content string"}""")).
      cast(DataFrameSegmentSpell("""{"InputCol":"content","OutputCol":"segmented"}""")).
      cast(DataFrameTFVectorizeSpell("""{"InputCol":"segmented","OutputCol":"tf"}"""))
//      cast(DataFrameSelectSpell().setSelectColNames("tf"))



  val vectorize2TFIDF =
    (vectorize2TF :+ IDFModelSource("""{"InputPath":"model/idf2"}""")).
      cast(DataFrameTFIDFVectorizeSpell("""{"InputCol":"tf", "OutputCol":"tfidf"}"""))

  val ldaModel = vectorize2TF.
    cast(LDATrainSpell("""{"InputCol":"tf","OutputCol":"docDist","K":3,"MaxIter":5,"Optimizer":"em"}"""))

  ldaModel.
    cast(ModelTarget("""{"OutputPath":"model/lda","Overwrite":true}"""))

//  val ldaModel = LDAModelSource("""{"InputPath":"model/lda","IsLocalModel":true}""")

  val ldaResult = (vectorize2TF :+ ldaModel).
      cast(LDATransformSpell("""{}"""))

}
