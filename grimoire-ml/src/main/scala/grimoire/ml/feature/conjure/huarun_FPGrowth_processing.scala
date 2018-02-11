package grimoire.ml.feature.conjure

import grimoire.spark.source.dataframe.HiveSource
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import grimoire.Implicits._
import grimoire.spark.globalSpark
import grimoire.util.json.reader.JsonReaders
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import scala.collection.mutable.ListBuffer

object huarun_FPGrowth_processing {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      appName("processing").
      enableHiveSupport().
      getOrCreate()


    val shop_user_map = HiveSource(args(0)).conjure.rdd.map {
      case Row(shopId,userIds) => (shopId.toString,userIds.asInstanceOf[String])
    }.map(x=>(x._1,x._2.split(" ").toSet)).collect.toMap

    val temp3 = HiveSource(args(1)).conjure.rdd.
      map{
        case Row(f1,f2,f3) => (f1.toString,f2.toString,f3.asInstanceOf[Double])
      }.collect.flatMap {
      x =>(shop_user_map(x._1)--shop_user_map(x._2)).toList.map(y=>(y,x._3)).map(y=>(x._1,x._2,y._1,y._2))
    }

    val result_temp = new ListBuffer[(String, String, String, Double)]()
    val temp_temp = new ListBuffer[String]()

    for(i<-temp3;if(! temp_temp.contains(i._2+""+i._3))){
      result_temp+=i
      temp_temp += (i._2+""+i._3)
    }

    val schema = StructType(
      Seq(
        StructField(name="antecedent",dataType = StringType,nullable = false),
        StructField(name="consequent",dataType = StringType,nullable = false),
        StructField(name="userId",dataType = StringType,nullable = false),
        StructField(name="confidence",dataType = DoubleType,nullable = false)
      ))

    val rdd2df = globalSpark.sparkContext.parallelize(result_temp.sortBy(x=>x._4)).map{
      case x => Row(x._1.toString,x._2.toString,x._3.toString,x._4.toDouble)
    }
    globalSpark.createDataFrame(rdd2df,schema).write.mode("overwrite").
      saveAsTable(JsonReaders.stringReader.read(args(2),"OutputHiveTable"))
  }
}
