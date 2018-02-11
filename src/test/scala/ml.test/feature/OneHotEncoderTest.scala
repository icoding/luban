package ml.test.feature

import grimoire.ml.feature.transform.DataFrameOneHotEncoderSpell

object OneHotEncoderTest extends App{
  import grimoire.Implicits._
  import grimoire.spark.globalSpark
  import org.apache.spark.sql.SparkSession
  import grimoire.spark.source.dataframe.CSVSource
  import grimoire.ml.feature.transform.{DataFrameIndexToStringSpell, DataFrameStringIndexerSpell}


  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()

  val s2OneHot = CSVSource("""{"InputPath":"file:///home/wuxiang/data/index.csv","Schema":"f1 double,str string"}""").
    cast(DataFrameStringIndexerSpell("""{"InputCol":"str","OutputCol":"index"}""")).
    cast(DataFrameOneHotEncoderSpell("""{"InputCol":"index","OutputCol":"categoryVec"}""")
      .setDropLast(false))

}
