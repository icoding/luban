package ml.test.feature

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.mllib.recommendation.{ALS,Rating}

object huarun_als_demo {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
  val df_temp = spark.sparkContext.textFile("file:///home/wuxiang/data/df_als_price.csv")
  val transactions: RDD[Row] = df_temp.map(_.split(",").toList).map(x => Row(x(0).toInt,x(1).toInt,x(2).toDouble))
  val schema = StructType(Seq(StructField(name = "shopId", dataType = IntegerType, nullable = false),
    StructField(name = "userId", dataType = IntegerType, nullable = false),
    StructField(name = "rating", dataType = DoubleType, nullable = false)))
  val df = spark.createDataFrame(transactions, schema)

  val ratings = df.rdd.map{
    case Row(shopId,userId,rating) => Rating(shopId.asInstanceOf[Int],userId.asInstanceOf[Int],rating.asInstanceOf[Double])
  }


  val rank = 10
  val lambda = 0.01
  val numIterations = 10
  val model = ALS.train(ratings,rank,numIterations,lambda)


  val result = ratings.map(_.user).distinct().collect.flatMap {
    user => model.recommendProducts(user,100)
  }

  val temp_rec = spark.sparkContext.parallelize(result).map{
    case Rating(shopId,userId,rating) => (shopId+" "+userId,rating)
  }
  val temp_origin = ratings.map{
    case Rating(shopId,userId,rating) => (shopId+" "+userId,rating)
  }
  import spark.implicits._

  val result_toDF = temp_rec.subtractByKey(temp_origin).map(x=>(x._1.split(" "),x._2)).map{
    case (x,y) => (x(0).toInt,x(1).toInt,y)
  }.sortBy(x=>(x._1)).toDF

}
