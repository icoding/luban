package grimtest.test

import grimoire.spark.serializer.RowJsonSerializer
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by caphael on 2017/3/23.
  */
object JsonTest {
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()

  val rows = Seq(Row(1,"a"),Row(2,"b"),Row(3,null))
  val schema = StructType(
    Seq(
      StructField("col1",IntegerType),
      StructField("col2",StringType)
    )
  )

  val df = spark.createDataFrame(spark.sparkContext.parallelize(rows),schema)
  val s = new RowJsonSerializer
  df.rdd.map{
    case r=>
      s.serialize(r)
  }.collect()

}
