package grimtest.ml.feature

import grimoire.ml.feature.transform.{DataFrameOneHotEncoderSpell, DataFrameVectorIndexerSpell}

/**
  * Created by sjc505 on 17-6-23.
  */
object StringIndexerTest {
  import grimoire.Implicits._
  import grimoire.spark.globalSpark
  import org.apache.spark.sql.SparkSession
  import grimoire.spark.source.dataframe.CSVSource
  import grimoire.ml.feature.transform.{DataFrameIndexToStringSpell, DataFrameStringIndexerSpell}


  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()

  val si = CSVSource("""{"InputPath":"data/index.csv","Schema":"f1 double,str string"}""").
    cast(DataFrameStringIndexerSpell("""{"InputCol":"str","OutputCol":"index"}"""))

  val is =CSVSource("""{"InputPath":"data/index.csv","Schema":"f1 double,str string"}""").
    cast(DataFrameStringIndexerSpell("""{"InputCol":"str","OutputCol":"index"}""")).
    cast(DataFrameIndexToStringSpell("""{"InputCol":"index","OutputCol":"st2"}"""))

  val encode =CSVSource("""{"InputPath":"data/index.csv","Schema":"f1 double,str string"}""").
    cast(DataFrameStringIndexerSpell("""{"InputCol":"str","OutputCol":"index"}""")).
    cast(DataFrameOneHotEncoderSpell("""{"InputCol":"index","OutputCol":"encode"}"""))


}
