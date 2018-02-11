package grimoire.ml.test.evaluate

import grimoire.ml.parsing.transform.{RDDDoubleArray2DParsingSpell, RDDIntArray2DParsingSpell}
import grimoire.spark.source.rdd.TextFileRDDSource
import grimoire.spark.globalSpark
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.evaluate.MultilabelMetricsSpell

/**
  * Created by sjc505 on 17-7-6.
  */
object MultilabelMetricsTest {
  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()

  val dt = TextFileRDDSource("""{"InputPath":"data/multilabel.txt"}""").
    cast(RDDDoubleArray2DParsingSpell()).
    cast(MultilabelMetricsSpell())


}
