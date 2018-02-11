package grimtest.test

/**
  * Created by caphael on 2017/2/22.
  */
object PlanTest extends App{

  import scala.io.Source
  import grimoire.operation.Scroll
  import grimoire.Implicits._
  import grimoire.spark.globalSpark
  import org.apache.spark.sql.SparkSession
  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val scroll = new Scroll
  val jstr = Source.fromFile("plan/scroll2.json").mkString
  scroll.parseJson(jstr)


  scroll.conjureAll()

}
