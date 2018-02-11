package feature

import grimoire.spark.source.dataframe.HiveSource
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import grimoire.Implicits._
import grimoire.spark.globalSpark
import grimoire.util.json.reader.JsonReaders
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import scala.collection.mutable.ListBuffer

object huarun_processing {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("processing").
      enableHiveSupport().
      getOrCreate()
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)


    val shop_user_map = HiveSource("""{"InputHiveTable":"shop_user_rel"}""").conjure.rdd.map {
      case Row(shopId,userIds) => (shopId.toString,userIds.asInstanceOf[String])
    }.map(x=>(x._1,x._2.split(" ").toSet)).collect.toMap

    val temp3 = HiveSource("""{"InputHiveTable":"huarun_shop_rel_conjure"}""").conjure.rdd.
      map{
        case Row(f1,f2,f3) => (f1.toString,f2.toString,f3.asInstanceOf[Double])
      }.flatMap{
      x =>(shop_user_map(x._1)--shop_user_map(x._2)).toList.map(y=>(x._1,x._2,y,x._3))
    }.collect

    val result_temp = new ListBuffer[(String, String, String, Double)]()
    val temp_temp = new ListBuffer[String]()

    for(i<-temp3;if(! temp_temp.contains(i._2+""+i._3))){
      result_temp += i
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
      saveAsTable(JsonReaders.stringReader.read("""{"OutputHiveTable":"resultsssss"}""","OutputHiveTable"))
  }

}
