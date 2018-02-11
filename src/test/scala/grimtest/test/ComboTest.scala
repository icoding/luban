package grimtest.test

import grimoire.ml.feature.transform.DataFrameTFVectorizeSpell
import grimoire.nlp.transform.DataFrameSegmentSpell
import grimoire.Implicits.jstr2JsValue
import grimoire.spark.globalSpark
import grimoire.spark.transform.rdd.{RDDLineSplitSpell, StringSeqRDDToDFSpell}
import org.apache.spark.sql.SparkSession

/**
  * Created by caphael on 2017/2/17.
  */
object ComboTest extends App{

//  spark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val s1 = RDDLineSplitSpell()
  val s2 = StringSeqRDDToDFSpell()
  val s3 = DataFrameSegmentSpell()
  val s4 = DataFrameTFVectorizeSpell()
  val c1 = s2 ~ s3
//  val c2 = s3 ~ s4
  val vectorize2TFCombo = s1 ~ c1 ~ s4

  val json = jstr2JsValue("""[{"Separator":"\\s+","NumField":2},{"Schema":"id int,content string"},{"InputCol":"content","OutputCol":"segmented"},{"InputCol":"segmented","OutputCol":"tf"}]""")
  vectorize2TFCombo.parseJson(json)

}
