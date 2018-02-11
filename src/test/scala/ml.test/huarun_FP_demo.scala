package ml.test


import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import scala.collection.mutable.ListBuffer


object huarun_FP_demo {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
  val df_temp = spark.sparkContext.textFile("file:///home/wuxiang/data/df_shao_rel.csv")
  val transactions:RDD[Row] = df_temp.map(_.split(",").toList).map(x=>Row(x(0).toInt,x(1)))
  val schema = StructType(Seq(StructField(name="uid",dataType = IntegerType,nullable = false),
    StructField(name="shopId",dataType = StringType,nullable = false)))
  val df = spark.createDataFrame(transactions,schema)


  val df_rdd = df.select("shopId").rdd.map {
    case Row(x) => x.asInstanceOf[String]
  }.map(_.trim.split(" "))
  val fpg = new FPGrowth().setMinSupport(0.02)
  val model = fpg.run(df_rdd)


  val minConfidence = 0.45
  val temp = model.generateAssociationRules(minConfidence).filter(_.antecedent.length==1).
    map(x=>(x.antecedent.mkString(""),x.consequent.mkString(""),x.confidence)).sortBy(x=> - x._3).
    map(x=>Row(x._1.toString,x._2.toString,x._3.toDouble))
  val schema2 = StructType(Seq(StructField(name="antecedent",dataType = StringType,nullable = false),
    StructField(name="consequent",dataType = StringType,nullable = false),
    StructField(name="confidence",dataType = DoubleType,nullable = false)))

  val result = spark.createDataFrame(temp,schema2)




  /**
    * load data to hive table
    */
  val shop_user = spark.sparkContext.textFile("file:///home/wuxiang/data/shop_user.csv")
  val transactions2:RDD[Row] = shop_user.map(_.split(",").toList).map(x=>Row(x(0).toInt,x(1)))
  val schema3 = StructType(Seq(StructField(name="shopId",dataType = IntegerType,nullable = false),
    StructField(name="userId",dataType = StringType,nullable = false)))
  val df3 = spark.createDataFrame(transactions2,schema3)


  /**
    *data processing
    */
  val shop_user_rdd = df3.rdd.map {
    case Row(shopId,userIds) => (shopId.toString,userIds.asInstanceOf[String])
  }.map(x=>(x._1,x._2.split(" ").toSet))

  val temp2  = model.generateAssociationRules(minConfidence).filter(_.antecedent.length==1).
    map(x=>(x.antecedent.mkString(""),x.consequent.mkString(""),x.confidence)).sortBy(x=> - x._3)
  val shop_user_map = shop_user_rdd.collect.toMap

  val temp3 = temp2.collect.flatMap{
    case x =>
      (shop_user_map(x._2)--shop_user_map(x._1)).toList.map(y=>(y,x._3)).map(y=>(x._1,x._2,y._1,y._2))
  }
  val result_temp = new ListBuffer[(String, String, String, Double)]()
  val temp_temp = new ListBuffer[String]()
  for(i<-temp3;if(! temp_temp.contains(i._2+""+i._3))){
    result_temp+=i
    temp_temp += (i._2+""+i._3)
  }

  import spark.implicits._
  spark.sparkContext.parallelize(result_temp.sortBy(x=>x._2)).toDF.show


}
