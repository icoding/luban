package ml.test.classify

import grimoire.spark.globalSpark
import org.apache.spark.ml.feature.{Bucketizer, QuantileDiscretizer, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, udf}

object tantic_demo {
  globalSpark = SparkSession.
    builder().
    master("local[4]").
    appName("lr").
    enableHiveSupport().
    getOrCreate()



  val df = globalSpark.read.format("csv").option("header",true).option("inferSchema",true).load("file:///home/wuxiang/data/tantic/train.csv")
  val indexer = new StringIndexer().setInputCol("Sex").setOutputCol("Sex_n").fit(df).transform(df)
  val indexer2 = new StringIndexer().setInputCol("Embarked").setOutputCol("Embarked_n").
    fit(indexer).transform(indexer)
  val splits = Array(Double.NegativeInfinity, 18, 25, 35, 60, Double.PositiveInfinity)
  val bucketizer = new Bucketizer().
    setInputCol("Age").setOutputCol("Age_n").
    setSplits(splits).transform(indexer2)
  val discretizer = new QuantileDiscretizer().
    setInputCol("Fare").setOutputCol("Fare_n").setNumBuckets(10).
    fit(bucketizer).transform(bucketizer)




  val name = StructField("name", StringType, true)
  val number = StructField("number", StringType, true)
  val schema = StructType(Seq(name, number))

  val rddRow1 = globalSpark.sparkContext.parallelize(List(("wxuaf",25),("zhaofa",26))).map(x=>Row(x._1,x._2))
  globalSpark.createDataFrame(rddRow1,schema)


  val schema1 = StructType(
    Seq(
      StructField("name",StringType,true)
      ,StructField("age",IntegerType,true)
    )
  )
  val rowRDD2 = globalSpark.sparkContext.
    textFile("file:///home/wuxiang/data/people.txt",2).
    map( x => x.split(" ")).map( x => Row(x(0),x(1).trim().toInt))
  globalSpark.createDataFrame(rowRDD2,schema1).show

  val rowRdd3 = globalSpark.sparkContext.parallelize(List(("wxuaf",25),("zhaofa",26))).
    map(x=>Row(x._1,x._2))
  globalSpark.createDataFrame(rowRdd3,schema1).show

}
