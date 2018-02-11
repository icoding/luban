package grimoire.ml.feature.conjure
/**
  * Created by Aron on 17-11-20.
  */
/**
  * Created by Aron on 17-11-16.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.ml.feature.transform.DataFrameRandomSplit
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.util.json.reader.JsonReaders
import org.apache.spark.sql.SparkSession

object RandomSplit {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      appName("RandomSplit").
      enableHiveSupport().
      getOrCreate()

    val splitDf = HiveSource(args(0)).
      cast(DataFrameRandomSplit(args(1))).
      conjure
    val trainSet = JsonReaders.stringReader.read(args(2),"OutputTrainingSetTable")
    val testSet = JsonReaders.stringReader.read(args(3),"OutputTestSetTable")

    splitDf(0).toDF().write.mode("Overwrite").saveAsTable(trainSet)
    splitDf(1).toDF().write.mode("Overwrite").saveAsTable(testSet)

  }
}
