package grimtest.ml.dimensionalityreduction

import grimoire.spark.source.rdd.TextFileRDDSource
import grimoire.spark.globalSpark
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.dimensionalityreduction.SingularValueDecompositionSpell
import grimoire.ml.feature.transform.RDDSeqDoubleToVectorSpell
import grimoire.ml.parsing.transform.RDDDoubleSeqParsingSpell
/**
  * Created by sjc505 on 17-7-12.
  */
object SingularValueDecompositionTest {
  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val dt =
    TextFileRDDSource("""{"InputPath":"data/svd.txt"}""").
      cast(RDDDoubleSeqParsingSpell()).
      cast(RDDSeqDoubleToVectorSpell())

  val svd=dt.cast(SingularValueDecompositionSpell())

}
