package grimtest.test

/**
  * Created by caphael on 2017/2/18.
  */
object ReflectTest {
  import grimoire.spark.globalSpark
  import grimoire.operation.StuffFactory._
  import grimoire.Implicits._
  import org.apache.spark.sql.SparkSession

  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val i = _createPot("grimoire.spark.source.TextFileRDDSource","""{"InputPath":"data/test.txt"}""")
  val s1 = _createSpell("grimoire.spark.transform.rdd.RDDLineSplitSpell","""{"Separator":"\\s+","NumField":2}""")
  val s2 = _createSpell("grimoire.spark.transform.rdd.StringSeqRDDToDFSpell","""{"Schema":"id int,content string"}""")
  val s3 = _createSpell("grimoire.nlp.transform.DataFrameSegmentSpell","""{"InputCol":"content","OutputCol":"segmented"}""")
  val s4 = _createSpell("grimoire.ml.feature.transform.DataFrameTFVectorizeSpell","""{"InputCol":"segmented","OutputCol":"tf"}""")

  val wf = i.cast(s1).cast(s2).cast(s3).cast(s4)

}
