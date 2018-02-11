package grimtest.ml.feature

import grimoire.Implicits._
import grimoire.ml.feature.transform.{DataFramePolynomiaExpansionSpell, DataFrameStopWordsRemoverSpell, DataFrameTFVectorizeSpell, DataFrameWord2VecSpell}
import grimoire.nlp.transform.DataFrameSegmentSpell
import grimoire.spark.globalSpark
import org.apache.spark.sql.SparkSession
import grimoire.spark.source.rdd.TextFileRDDSource
import grimoire.spark.transform.rdd.{RDDLineSplitSpell, StringSeqRDDToDFSpell}

/**
  * Created by sjc505 on 17-6-21.
  */
object PolynomiaExpansionTest {
  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()

  val poly=
    TextFileRDDSource("""{"InputPath":"data/test.txt"}""").
      cast(RDDLineSplitSpell("""{"Separator":"\\s+","NumField":2}""")).
      cast(StringSeqRDDToDFSpell("""{"Schema":"id int,content string"}""")).
      cast(DataFrameSegmentSpell("""{"InputCol":"content","OutputCol":"segmented"}""")).
      cast(DataFrameStopWordsRemoverSpell("""{"InputCol":"segmented","OutputCol":"keyword"}""")).
      cast(DataFrameWord2VecSpell("""{"InputCol":"segmented","OutputCol":"wordvector"}""").setMinCount(0).setVectorSize(3)).
      cast(DataFramePolynomiaExpansionSpell().setInputCol("wordvector").setOutputCol("poly").setDegree(3))

}
