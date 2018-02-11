package classify
/**
  * Created by Aron on 17-11-6.
  */
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.classify.source.DecisionTreeModelSource
import grimoire.ml.classify.transform.DataFrameDecisionTreeClassifierPredictSpell

object DecisionTreePre {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      appName("DecisionTreePre").
      master("local[1]").
      enableHiveSupport().
      getOrCreate()

    (HiveSource("""{"InputHiveTable":"DecisionTree"}""") :+ DecisionTreeModelSource("""{"InputModelSource":"hdfs:///user/model/DecisionTree"}""")).
      cast(DataFrameDecisionTreeClassifierPredictSpell()).
      cast(DataFrameTarget("""{"OutputHiveTable":"DecisionTreeTestPre"}""")).conjure
  }
}
