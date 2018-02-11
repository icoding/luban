package grimtest.ml.FPmining

import grimoire.spark.globalSpark
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.FPMining.{PrefixSpanModelSpell, PrefixSpanSpell}
import grimoire.ml.parsing.transform.RDDIntArray2DParsingSpell
import grimoire.spark.source.rdd.TextFileRDDSource


/**
  * Created by sjc505 on 17-6-30.
  */
object PrefixSpanTest {
  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()

  val dt = TextFileRDDSource("""{"InputPath":"data/prefixspan.txt"}""").
   cast(RDDIntArray2DParsingSpell())
  val mod = dt.
    cast(PrefixSpanModelSpell().setMinSupport(0.5).setMaxPatternLength(5))

  val ps = (dt :+ mod).cast(PrefixSpanSpell())
}
