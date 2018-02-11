package grimtest.ml.filtering

import grimoire.spark.source.dataframe.CSVSource
import grimoire.spark.globalSpark
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.filtering.transform.{DataFrameALSPredictSpell, DataFrameALSTrainSpell}

/**
  * Created by sjc505 on 17-6-29.
  */
object ALSTest {
  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()

  val df = CSVSource("""{"InputPath":"data/movie.csv","Schema":"userId double,movieId double,rating double"}""")

  val mod = df.cast(DataFrameALSTrainSpell().setMaxIter(5).setRegParam(0.01).setUserCol("userId").setItemCol("movieId").setRatingCol("rating"))

//  val pre = (df :+ mod).cast(DataFrameALSPredictSpell().setUserCol("userId").setItemCol("movieId").setRatingCol("rating").setPredictionCol("pre"))

}
