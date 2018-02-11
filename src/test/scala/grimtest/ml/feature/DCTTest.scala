package grimtest.ml.feature

/**
  * Created by sjc505 on 17-6-21.
  */

import grimoire.Implicits._
import grimoire.ml.feature.transform._
import grimoire.nlp.transform.DataFrameSegmentSpell
import grimoire.spark.globalSpark
import org.apache.spark.sql.SparkSession
import grimoire.spark.source.rdd.TextFileRDDSource
import grimoire.spark.transform.rdd.{RDDLineSplitSpell, StringSeqRDDToDFSpell}

/**
  * Created by sjc505 on 17-6-21.
  */
object  DCTTest {
  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()

  val dct=
    TextFileRDDSource("""{"InputPath":"data/test.txt"}""").
      cast(RDDLineSplitSpell("""{"Separator":"\\s+","NumField":2}""")).
      cast(StringSeqRDDToDFSpell("""{"Schema":"id int,content string"}""")).
      cast(DataFrameSegmentSpell("""{"InputCol":"content","OutputCol":"segmented"}""")).
      cast(DataFrameStopWordsRemoverSpell("""{"InputCol":"segmented","OutputCol":"keyword"}""")).
      cast(DataFrameWord2VecSpell("""{"InputCol":"segmented","OutputCol":"wordvector"}""").setMinCount(0).setVectorSize(3)).
      cast(DataFrameDCTSpell().setInputCol("wordvector").setOutputCol("dct").setInverse(false))

}
