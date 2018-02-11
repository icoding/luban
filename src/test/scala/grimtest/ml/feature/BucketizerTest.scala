package grimtest.ml.feature

import grimoire.ml.feature.transform.{DataFrameBucketizerSpell, DataFrameDCTSpell, DataFrameStopWordsRemoverSpell, DataFrameWord2VecSpell}
import grimoire.nlp.transform.DataFrameSegmentSpell
import grimoire.spark.source.rdd.TextFileRDDSource
import grimoire.spark.globalSpark
import grimoire.spark.transform.rdd.{RDDLineSplitSpell, StringSeqRDDToDFSpell}
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
/**
  * Created by sjc505 on 17-6-22.
  */
object BucketizerTest {
  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()
  val bt=
    TextFileRDDSource("""{"InputPath":"data/test.txt"}""").
      cast(RDDLineSplitSpell("""{"Separator":"\\s+","NumField":2}""")).
      cast(StringSeqRDDToDFSpell("""{"Schema":"id int,content string"}""")).
      cast(DataFrameSegmentSpell("""{"InputCol":"content","OutputCol":"segmented"}""")).
      cast(DataFrameStopWordsRemoverSpell("""{"InputCol":"segmented","OutputCol":"keyword"}""")).
      cast(DataFrameWord2VecSpell("""{"InputCol":"segmented","OutputCol":"wordvector"}""").setMinCount(0).setVectorSize(3)).
      cast(DataFrameBucketizerSpell().setInputCol("wordvector").setOutputCol("bt").setSplits(Seq(-1.0,0.0,1.0)))

}
