package grimtest.spark

/**
  * Created by caphael on 2017/6/22.
  */


object UDFTest extends App{
  import org.apache.spark.sql.SparkSession
  import grimoire.spark.globalSpark
  import grimoire.Implicits._
  import org.apache.spark.sql.functions.{col, udf}
  import grimoire.spark.function.createArrayByColumnsFunction


  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()
  val dat = Seq(
    (1,2,3,4,"aaa"),
    (5,6,7,8,"bbb"),
    (9,10,11,12,"ccc")
  )
  val df = globalSpark.createDataFrame(dat).toDF("a","b","c","d","label")

  val udfFunc = createArrayByColumnsFunction()
  val res =df.select(udfFunc(col("a"),col("b")))


  import org.apache.spark.sql.Row
  import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}



}
