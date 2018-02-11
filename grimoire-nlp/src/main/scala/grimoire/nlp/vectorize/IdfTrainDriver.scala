//package grimoire.nlp.vectorize
//
//import caphael.common.cli.{ConfigBase, HasIOput, OptionParser, ParseIOput}
//import com.baifendian.nlp.segment.Segmenter
//import com.baifendian.nlp.segment.jieba.{JiebaSegLike, MergePostLike}
//import com.baifendian.spark.SparkAppLike
//import org.apache.spark.ml.feature.{HashingTF, IDF}
//import org.apache.spark.ml.linalg.Vector
//import org.apache.spark.sql.functions._
//
///**
//  * Created by caphael on 2016/10/30.
//  */
//object IdfTrainDriver extends SparkAppLike{
//
//  case class Config() extends ConfigBase with HasIOput
//  override type CT = Config
//
//  override def appName: String = "IDF Model Training"
//
//  override var CONFIG: CT = new Config
//
//  override def parserSetup: OptionParser[CT] = {
//    val parser = new OptionParser[CT](appName) with ParseIOput[CT]{}
//
//    parser.inputParser.text("Input File Path")
//    parser.outputParser.text("IDF Model Output Path")
//
//    parser
//  }
//
//  override def runBody(): Unit = {
//
//    val input = SC.textFile(CONFIG.input)
//
//    val segmenter = (new Segmenter() with JiebaSegLike with MergePostLike).toFilter(false).toDrop(false)
//
//    val segs = input.map(_.split(Sep,2) match {case Array(x,y) => (x,segmenter.segment(y).map(_._1))})
//
//    val segDF = SQLC.createDataFrame(segs).toDF("id","words")
//
//    val tfVectorizer = new TfVectorizer().setInputCol("words").setOutputCol("tf")
//
//    val tfVecs = tfVectorizer.vectorize(segDF)
//
//    val model = new IDF().setMinDocFreq(1).setInputCol("tf").setOutputCol("tfidf").fit(tfVecs)
//
//    model.save(CONFIG.output)
//
//  }
//}
