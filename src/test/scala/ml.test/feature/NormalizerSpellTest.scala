package ml.test.feature

import grimoire.ml.feature.transform.{DataFrameBucketizerSpell, DataFrameMinMaxScalerSpell, DataFrameQuantileDiscretizerSpell}

object NormalizerSpellTest {
  import grimoire.Implicits._
  import grimoire.spark.globalSpark
  import org.apache.spark.sql.{DataFrame, SparkSession}
  import grimoire.spark.source.dataframe.CSVSource
  import grimoire.ml.feature.transform.DataFrameVectorAssemblerSpell
  import grimoire.ml.feature.transform.{DataFrameNormalizerSpell, DataFrameStandardScalerSpell}


  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val df = CSVSource("""{"InputPath":"data/mlib_wx/testt.csv","Schema":"f1 double,f2 double,f3 double"}""").
    cast(DataFrameVectorAssemblerSpell().setInputCols(Seq("f1","f2","f3")).setOutputCol("features")).
    cast(DataFrameNormalizerSpell().setInputCol("features").setOutputCol("normFeatures").setP(1.0))

  val df_std = CSVSource("""{"InputPath":"data/mlib_wx/testt.csv","Schema":"f1 double,f2 double,f3 double"}""").
    cast(DataFrameVectorAssemblerSpell().setInputCols(Seq("f1","f2","f3")).setOutputCol("features")).
    cast(DataFrameStandardScalerSpell().setInputCol("features").setOutputCol("standardscaler"))

//  val dfMinMax = CSVSource("""{"InputPath":"data/mlib_wx/testt.csv","Schema":"f1 double,f2 double,f3 double"}""").
//    cast(DataFrameVectorAssemblerSpell().setInputCols(Seq("f1","f2","f3")).setOutputCol("features")).
//    cast(DataFrameMinMaxScalerSpell().setInputCol("features").setOutputCol("standardscaler")).
//    cast(DataFrameQuantileDiscretizerSpell().setInputCol("f1").setOutputCol("bucket").setNumBuckets(3))


}
