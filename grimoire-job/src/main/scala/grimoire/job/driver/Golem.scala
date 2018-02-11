//package grimoire.job.driver
//
//import grimoire.job.configuration.{HasPlanFile, HasScrollName}
//import grimoire.operation.Scroll
//import org.apache.spark.SparkContext
//import org.apache.spark.sql.{SQLContext, SparkSession}
//import puck.app.AbstractApp
//import puck.spark.configuration.{HasEnableHiveSupport, HasMaster}
//
///**
//  * Created by caphael on 2017/2/28.
//  */
//object Golem extends AbstractApp
//  with App
//  with HasMaster
//  with HasScrollName
//  with HasPlanFile
//  with HasEnableHiveSupport
//{
//  //  spark 2.0
//  var SS:SparkSession = null
//  var SC:SparkContext = null
//  var SQLC:SQLContext = null
//
//  private var toStop:Boolean = false
//
//  override def setup(): Boolean = {
//    //    spark 2.0
//    if (this.SS == null){
//      val builder = SparkSession.builder().appName(this.appName).master(getMaster)
//
//      SS = {
//        if (getEnableHiveSupport())
//          builder.enableHiveSupport()
//        else
//          builder
//      }.getOrCreate()
//
//      toStop = true
//    }
//    SC = SS.sparkContext
//    SQLC = SS.sqlContext
//    true
//  }
//
//  override protected def runBody(): Boolean = {
//    spark = SS
//    val scroll = Scroll.load(getPlanFile)
//    scroll.conjureAll()
//    true
//  }
//
//  override protected def appName: String = "Golem"
//
//  run(args)
//
//  override protected def cleanup(): Boolean = {
//    if ( this.toStop ){
//      this.SS.stop()
//    }
//    true
//  }
//}
