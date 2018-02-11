package grimtest.zeppelin

/**
  * Created by caphael on 2017/7/19.
  */
object ZeppelinLabeledMatrixTest extends App{
  import grimoire.spark.globalSpark
  import grimoire.spark.source.dataframe.CSVSource
  import org.apache.spark.sql.SparkSession
  import grimoire.Implicits._
  import grimoire.ml.statistics.DataFrameCorrelationsSpell
  import grimoire.zeppelin.target.ZeppelinLabeledMatrixTarget
  import grimoire.zeppelin.transform.ZeppelinLabeledMatrixSpell

  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()

  val df = CSVSource("""{"InputPath":"data/iris.csv","Schema":"f1 double,f2 double,f3 double,f4 double,label string"}""")

  val cor= df.cast(DataFrameCorrelationsSpell().setInputCols(Seq("f1","f2","f3","f4")))
//  cor.cast(ZeppelinLabeledMatrixSpell().setThreshold(0.6)).conjure

}
