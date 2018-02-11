package grimtest.ml.model

import grimoire.ml.feature.transform.{DataFrameRandomSplitSpell, TakeDFSpell}
import grimoire.ml.model.{ DataFrameTrainValidationSplitPredictSpell, DataFrameTrainValidationSplitTrainSpell}
import grimoire.spark.source.dataframe.LibSvmSource
import grimoire.spark.globalSpark
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-7-3.
  */
object TrainValidationSplitTest {
  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val data = LibSvmSource("""{"InputPath":"data/mllib/sample_libsvm_data.txt"}""")
  val train = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.9)).cast(TakeDFSpell())
  val test = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.1)).cast(TakeDFSpell().setTakeTrain(false))

  val mod = train.cast(DataFrameTrainValidationSplitTrainSpell().setTrainRatio(0.8))
  val tv = (test :+ mod).cast(DataFrameTrainValidationSplitPredictSpell())


}
