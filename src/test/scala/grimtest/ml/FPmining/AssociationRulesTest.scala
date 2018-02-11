package grimtest.ml.FPmining

import grimoire.ml.FPMining.{AssociationRulesSpell, FPGrowthModelSpell, FPGrowthSpell, FreqItemSpell}
import grimoire.spark.source.rdd.TextFileRDDSource
import grimoire.spark.globalSpark
import grimoire.spark.transform.rdd.RDDLineSplitToArraySpell
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
/**
  * Created by sjc505 on 17-6-30.
  */
object AssociationRulesTest {

  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()
  val freq = TextFileRDDSource("""{"InputPath":"data/fpgrowth.txt"}""").
    cast(RDDLineSplitToArraySpell("""{"Separator":"\\s+"}""")).
    cast(FPGrowthModelSpell().setMinSupport(0.2).setNumPartitions(10)).
    cast(FreqItemSpell())
  val rules = freq.cast(AssociationRulesSpell())
}
