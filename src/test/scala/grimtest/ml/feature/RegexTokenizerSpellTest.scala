package grimtest.ml.feature

import grimoire.spark.source.rdd.TextFileRDDSource
import grimoire.spark.globalSpark
import grimoire.spark.transform.rdd.{RDDLineSplitSpell, StringSeqRDDToDFSpell}
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.feature.transform.DataFrameRegexTokenizerSpell

/**
  * Created by sjc505 on 17-6-23.
  */
object RegexTokenizerSpellTest {

  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val rt =
    TextFileRDDSource("""{"InputPath":"data/test.txt"}""").
      cast(RDDLineSplitSpell("""{"Separator":"\\s+","NumField":2}""")).
      cast(StringSeqRDDToDFSpell("""{"Schema":"id int,content string"}""")).
      cast(DataFrameRegexTokenizerSpell("""{"InputCol":"content","OutputCol":"rtokenizer"}"""))


}
