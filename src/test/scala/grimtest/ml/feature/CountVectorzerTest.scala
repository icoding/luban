package grimtest.ml.feature

import grimoire.ml.feature.transform.{DataFrameCountVectorzerSpell, DataFrameStopWordsRemoverSpell}
import grimoire.nlp.transform.DataFrameSegmentSpell
import grimoire.spark.source.rdd.TextFileRDDSource
import grimoire.spark.globalSpark
import grimoire.spark.transform.rdd.{RDDLineSplitSpell, StringSeqRDDToDFSpell}
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._


/**
  * Created by sjc505 on 17-6-23.
  */
object CountVectorzerTest {
  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val cv =
    TextFileRDDSource("""{"InputPath":"data/test.txt"}""").
      cast(RDDLineSplitSpell("""{"Separator":"\\s+","NumField":2}""")).
      cast(StringSeqRDDToDFSpell("""{"Schema":"id int,content string"}""")).
      cast(DataFrameSegmentSpell("""{"InputCol":"content","OutputCol":"segmented"}""")).
      cast(DataFrameStopWordsRemoverSpell("""{"InputCol":"segmented","OutputCol":"keyword"}""")).
      cast(DataFrameCountVectorzerSpell("""{"InputCol":"keyword","OutputCol":"cv"}""").setVocabSize(3).setMinDF(0.0))


}
