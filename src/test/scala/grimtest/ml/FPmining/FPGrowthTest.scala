package grimtest.ml.FPmining

import grimoire.spark.source.rdd.TextFileRDDSource
import grimoire.spark.globalSpark
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.FPMining.{FPGrowthModelSpell, FPGrowthSpell}
import grimoire.spark.transform.rdd.{RDDLineSplitToArraySpell}

/**
  * Created by sjc505 on 17-6-29.
  */
object FPGrowthTest {
  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()
  val dt = TextFileRDDSource("""{"InputPath":"data/fpgrowth.txt"}""").
    cast(RDDLineSplitToArraySpell("""{"Separator":"\\s+"}"""))


  val mod = dt.
    cast(FPGrowthModelSpell().setMinSupport(0.2).setNumPartitions(10)).
    cast(FPGrowthSpell())

}
