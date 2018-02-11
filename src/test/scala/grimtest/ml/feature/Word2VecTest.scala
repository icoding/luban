package grimtest.ml.feature

import grimoire.ml.feature.transform._
import org.apache.spark.sql.SparkSession
import grimoire.nlp.transform.DataFrameSegmentSpell
import grimoire.spark.transform.rdd.StringSeqRDDToDFSpell
import grimoire.Implicits._
import grimoire.spark.globalSpark
import grimoire.spark.transform.rdd.RDDLineSplitSpell
import grimoire.spark.source.rdd.TextFileRDDSource

/**
  * Created by sjc505 on 17-6-20.
  */
object Word2VecTest {

  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val wordvector =
    TextFileRDDSource("""{"InputPath":"data/test.txt"}""").
      cast(RDDLineSplitSpell("""{"Separator":"\\s+","NumField":2}""")).
      cast(StringSeqRDDToDFSpell("""{"Schema":"id int,content string"}""")).
      cast(DataFrameSegmentSpell("""{"InputCol":"content","OutputCol":"segmented"}""")).
      cast(DataFrameStopWordsRemoverSpell("""{"InputCol":"segmented","OutputCol":"keyword"}""")).
      cast(DataFrameWord2VecSpell("""{"InputCol":"segmented","OutputCol":"wordvector"}""").setMinCount(0).setVectorSize(3))
  val minmax = wordvector.
      cast(DataFrameMinMaxScalerSpell().setInputCol("wordvector").setOutputCol("minmax"))

  val maxabs = wordvector.
      cast(DataFrameMaxAbsScalerSpell().setInputCol("wordvector").setOutputCol("maxabs"))

  val pca = wordvector.
    cast(DataFramePCASpell().setInputCol("wordvector").setOutputCol("pca").setK(3))

  val vi =wordvector.
    cast(DataFrameVectorIndexerSpell("""{"InputCol":"wordvector","OutputCol":"vi"}""").setMaxCategories(2))

  val lsh =wordvector.
    cast(DataFrameBucketedRandomProjectionLSHSpell("""{"InputCol":"wordvector","OutputCol":"lsh"}""").setBucketLength(2.0).setNumHashTables(3))

  val mlsh =wordvector.
    cast(DataFrameMinHashLSHSpell("""{"InputCol":"wordvector","OutputCol":"mlsh"}""").setNumHashTables(3))

  //  val ele= wordvector.
//      cast(DataFrameElementwiseProductSpell().setInputCol("wordvector").setOutputCol("ele"))


}
