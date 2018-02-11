package ml.test.feature

import grimoire.ml.feature.transform.{DataFrameChiSqSelectorSpell, DataFrameStringIndexerSpell}

object ChiSqSelectorTest extends App{
  import grimoire.spark.globalSpark
  import org.apache.spark.sql.SparkSession
  import grimoire.Implicits._
  import grimoire.spark.source.dataframe.CSVSource
  import grimoire.ml.feature.transform.{DataFrameInteractionSpell, DataFrameVectorAssemblerSpell}


  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val vecChiSelected = CSVSource("""{"InputPath":"data/iris.csv","Schema":"f1 double,f2 double,f3 double,f4 double,label string"}""").
    cast(DataFrameVectorAssemblerSpell().setInputCols(Seq("f1","f2","f3","f4")).setOutputCol("vec1")).
    cast(DataFrameStringIndexerSpell().setInputCol("label").setOutputCol("labelNumber")).
    cast(DataFrameChiSqSelectorSpell().setLabelCol("labelNumber").setFeaturesCol("vec1").setNumTopFeatures(2).setOutputCol("selectedFeature"))

  vecChiSelected.conjure.show
}
