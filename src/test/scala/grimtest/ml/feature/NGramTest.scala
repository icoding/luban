package grimtest.ml.feature

import grimoire.ml.feature.transform.{DataFrameNGramSpell}
import org.apache.spark.sql.SparkSession
import grimoire.nlp.transform.DataFrameSegmentSpell
import grimoire.spark.transform.rdd.StringSeqRDDToDFSpell
import grimoire.Implicits._
import grimoire.spark.globalSpark
import grimoire.spark.transform.rdd.RDDLineSplitSpell
import grimoire.spark.source.rdd.TextFileRDDSource

/**
  * Created by sjc505 on 17-6-21.
  */
object NGramTest {

  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val ngram=
   TextFileRDDSource("""{"InputPath":"data/test.txt"}""").
     cast(RDDLineSplitSpell("""{"Separator":"\\s+","NumField":2}""")).
     cast(StringSeqRDDToDFSpell("""{"Schema":"id int,content string"}""")).
     cast(DataFrameSegmentSpell("""{"InputCol":"content","OutputCol":"segmented"}""")).
     cast(DataFrameNGramSpell("""{"InputCol":"segmented","OutputCol":"n-gram"}""").setN(2))

}
