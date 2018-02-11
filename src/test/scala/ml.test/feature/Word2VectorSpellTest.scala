package ml.test.feature

/**
  * Created by Arno on 2017/10/17.
  */
import grimoire.spark.transform.rdd.StringSeqRDDToDFSpell
import org.apache.spark.sql.SparkSession
import grimoire.ml.feature.transform.DataFrameWord2VecSpell
import grimoire.nlp.transform.DataFrameSegmentSpell
import grimoire.Implicits._
import grimoire.spark.source.rdd.TextFileRDDSource
import grimoire.spark.globalSpark
import grimoire.spark.transform.rdd.RDDLineSplitSpell

object Word2VectorSpellTest extends App{
  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()

  val vectorize2Seg =
    TextFileRDDSource("""{"InputPath":"data/mllib/MLtest.txt"}""").
      cast(RDDLineSplitSpell("""{"Separator":"\\s+","NumField":2}""")).
      cast(StringSeqRDDToDFSpell("""{"Schema":"lable double,sentence string"}""")).
      cast(DataFrameSegmentSpell("""{"InputCol":"sentence","OutputCol":"segmented"}"""))

  vectorize2Seg.conjure.show(false)

  val word2Vec = vectorize2Seg.
    cast(DataFrameWord2VecSpell().setInputCol("segmented").setOutputCol("word2vec").setMinCount(1))

  word2Vec.conjure.show(false)
}
