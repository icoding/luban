package grimtest.ml.model

import grimoire.ml.feature.transform.{DataFrameRandomSplitSpell,TakeDFSpell}
import grimoire.spark.source.dataframe.{CSVSource, LibSvmSource}
import grimoire.spark.globalSpark
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.model.{DataFrameCrossValidatorPredictSpell, DataFrameCrossValidatorTrainSpell}


/**
  * Created by sjc505 on 17-7-3.
  */
object CrossValidatorTest {
  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val data = LibSvmSource("""{"InputPath":"data/mllib/sample_libsvm_data.txt"}""")
  val train = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.7)).cast(TakeDFSpell())
  val test = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.3)).cast(TakeDFSpell().setTakeTrain(false))

  val mod = train.cast(DataFrameCrossValidatorTrainSpell().setNumFolds(2))
  val cv = (test :+ mod).cast(DataFrameCrossValidatorPredictSpell())

}
