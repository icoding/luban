package filtering
/**
  * Created by Aron on 17-11-8.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.filtering.transform.DataFrameALSTrainSpell
import grimoire.ml.target.ModelTarget

object ALS {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("ALS").
      enableHiveSupport().
      getOrCreate()

    HiveSource("""{"InputHiveTable":"ALS"}""").
      cast(DataFrameALSTrainSpell("""{"MaxIter":5,"RegParam":0.01,"UserCol":"userId","ItemCol":"movieId","RatingCol":"rating","PredictionCol":"prediction"}""")).
      cast(ModelTarget("""{"OutputModelTarget":"hdfs:///user/model/ALS","Overwrite":true}""")).
      conjure
  }

}

