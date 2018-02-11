package grimtest.ml.feature

import grimoire.ml.feature.transform.DataFrameStopWordsRemoverSpell


/**
  * Created by sjc505 on 17-6-19.
  */
object StopWordsRemoverTest {

  import org.apache.spark.sql.SparkSession

  import grimoire.nlp.transform.DataFrameSegmentSpell
  import grimoire.spark.transform.rdd.StringSeqRDDToDFSpell
  import grimoire.Implicits._
  import grimoire.spark.globalSpark
  import grimoire.spark.transform.rdd.RDDLineSplitSpell
  import grimoire.spark.source.rdd.TextFileRDDSource


  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val keyword =
    TextFileRDDSource("""{"InputPath":"data/test.txt"}""").
      cast(RDDLineSplitSpell("""{"Separator":"\\s+","NumField":2}""")).
      cast(StringSeqRDDToDFSpell("""{"Schema":"id int,content string"}""")).
      cast(DataFrameSegmentSpell("""{"InputCol":"content","OutputCol":"segmented"}""")).
      cast(DataFrameStopWordsRemoverSpell("""{"InputCol":"segmented","OutputCol":"keyword"}"""))


}
