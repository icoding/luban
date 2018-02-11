package grimtest.ml.feature

import grimoire.ml.feature.transform._
import grimoire.nlp.transform.DataFrameSegmentSpell
import grimoire.spark.source.rdd.TextFileRDDSource
import grimoire.spark.globalSpark
import grimoire.spark.transform.rdd.{RDDLineSplitSpell, StringSeqRDDToDFSpell}
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-6-22.
  */
object MinMaxScalerTest {
  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()
  val wordvector=
    TextFileRDDSource("""{"InputPath":"data/test.txt"}""").
      cast(RDDLineSplitSpell("""{"Separator":"\\s+","NumField":2}""")).
      cast(StringSeqRDDToDFSpell("""{"Schema":"id int,content string"}""")).
      cast(DataFrameSegmentSpell("""{"InputCol":"content","OutputCol":"segmented"}""")).
      cast(DataFrameStopWordsRemoverSpell("""{"InputCol":"segmented","OutputCol":"keyword"}""")).
      cast(DataFrameWord2VecSpell("""{"InputCol":"segmented","OutputCol":"wordvector"}""").setMinCount(0).setVectorSize(3))

  val minmax=
    wordvector.
      cast(DataFrameMinMaxScalerSpell().setInputCol("wordvector").setOutputCol("minmax"))

  val maxabs=
    wordvector.
      cast(DataFrameMaxAbsScalerSpell().setInputCol("wordvector").setOutputCol("maxabs"))

}
