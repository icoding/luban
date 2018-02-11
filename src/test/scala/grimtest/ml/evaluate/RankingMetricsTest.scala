package grimoire.ml.test.evaluate

import grimoire.ml.evaluate.{DataFrameRankingMetricsSpell, RankingMetricsSpell}
import grimoire.ml.parsing.transform.RDDDoubleArray2DParsingSpell
import grimoire.spark.source.rdd.TextFileRDDSource
import grimoire.spark.globalSpark
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.feature.transform.DataFrameVectorAssemblerSpell
import grimoire.ml.filtering.transform.{DataFrameALSPredictSpell, DataFrameALSTrainSpell}
import grimoire.spark.source.dataframe.CSVSource

/**
  * Created by sjc505 on 17-7-6.
  */
object RankingMetricsTest {

  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()

  val dt = TextFileRDDSource("""{"InputPath":"data/multilabel.txt"}""").
    cast(RDDDoubleArray2DParsingSpell()).
    cast(RankingMetricsSpell())
//  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()
//
//  val df = CSVSource("""{"InputPath":"data/multilabel.csv","Schema":"label1 double,label2 double,pre1 double,pre2 double"}""").
//    cast(DataFrameVectorAssemblerSpell().setInputCols(Seq("label1","label2")).setOutputCol("label")).
//    cast(DataFrameVectorAssemblerSpell().setInputCols(Seq("pre1","pre2")).setOutputCol("pre"))
//
//  val ev = df.cast(DataFrameRankingMetricsSpell().setPredictionCol("pre").setLabelCol("label"))
}
