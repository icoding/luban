package grimtest.ml.feature

import grimoire.ml.feature.transform.{DataFrameNormalizerSpell, DataFrameStandardScalerSpell, DataFrameStopWordsRemoverSpell, DataFrameWord2VecSpell}
import grimoire.nlp.transform.DataFrameSegmentSpell
import grimoire.spark.source.rdd.TextFileRDDSource
import grimoire.spark.transform.rdd.{RDDLineSplitSpell, StringSeqRDDToDFSpell}
import grimoire.Implicits._
import grimoire.spark.globalSpark
import org.apache.spark.sql.SparkSession


/**
  * Created by sjc505 on 17-6-22.
  */
object StandardScalarTest {
  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val sta=
    TextFileRDDSource("""{"InputPath":"data/test.txt"}""").
    cast(RDDLineSplitSpell("""{"Separator":"\\s+","NumField":2}""")).
    cast(StringSeqRDDToDFSpell("""{"Schema":"id int,content string"}""")).
    cast(DataFrameSegmentSpell("""{"InputCol":"content","OutputCol":"segmented"}""")).
    cast(DataFrameStopWordsRemoverSpell("""{"InputCol":"segmented","OutputCol":"keyword"}""")).
    cast(DataFrameWord2VecSpell("""{"InputCol":"segmented","OutputCol":"wordvector"}""").setMinCount(0).setVectorSize(3)).
    cast(DataFrameStandardScalerSpell().setInputCol("wordvector").setOutputCol("sta").setWithMean(false).setWithStd(true))

}
