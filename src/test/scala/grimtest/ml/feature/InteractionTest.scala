package grimtest.ml.feature

import grimoire.ml.feature.transform.{DataFrameInteractionSpell, DataFrameVectorAssemblerSpell}


/**
  * Created by sjc505 on 17-6-26.
  */
object InteractionTest {
  import grimoire.spark.globalSpark
  import org.apache.spark.sql.SparkSession
  import grimoire.Implicits._
  import grimoire.spark.source.dataframe.CSVSource

  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val vec = CSVSource("""{"InputPath":"data/iris.csv","Schema":"f1 double,f2 double,f3 double,f4 double,label string"}""").
    cast(DataFrameVectorAssemblerSpell().setInputCols(Seq("f1","f2")).setOutputCol("vec1")).
    cast(DataFrameVectorAssemblerSpell().setInputCols(Seq("f3","f4")).setOutputCol("vec2")).
    cast(DataFrameInteractionSpell().setInputCols(Seq("vec1","vec2")).setOutputCol("vec"))

}
