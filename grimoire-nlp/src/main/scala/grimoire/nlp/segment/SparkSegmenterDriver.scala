//package grimoire.nlp.segment
//
///**
//  * Created by caphael on 2016/10/27.
//  */
//
//import caphael.common.cli.{ConfigBase, HasIOput, OptionParser, ParseIOput}
//import com.baifendian.cli.{HasFieldPos, HasSeparator, ParseFieldPos, ParseSeparator}
//import com.baifendian.nlp.segment.jieba.{JiebaSegLike, MergePostLike}
//import com.baifendian.spark.SparkAppLike
//
//import scala.collection.mutable.ArrayBuffer
//
//object SparkSegmenterDriver extends SparkAppLike{
//
//  case class Config(var nature:Boolean = false,var ner:Boolean = false) extends ConfigBase with HasIOput
//
//  override type CT = Config
//
//  override def appName: String = "SparkSegmenter"
//
//  override var CONFIG: CT = new Config
//
//  override def parserSetup: OptionParser[CT] = {
//    val parser = new OptionParser[CT](appName) with ParseIOput[CT]{
//      opt[Unit]('n',"nature") action { case(x,c:CT) =>
//        c.nature=true;c
//      } text("Tag natures") optional()
//      opt[Unit]("ner").abbr("ner") action { case(x,c:CT) =>
//        c.ner=true;c
//      } text("Turn NER on") optional()
//    }
//
//    parser.inputParser.text("Input Path of Text File")
//    parser.outputParser.text("Output Path of Segmented Text File")
//
//    parser
//  }
//
//  override def runBody(): Unit = {
//
//    val input = SC.textFile(CONFIG.input)
//
//    val segmenter = if(CONFIG.ner){
//      (new Segmenter() with JiebaSegLike with MergePostLike).toDrop(false).toFilter(true).natures(Set("ns","nr","nrt","t","nz","nt")).natureOut(CONFIG.nature)
//    }else{
//      (new Segmenter() with JiebaSegLike).toDrop(false).toFilter(false).natureOut(CONFIG.nature)
//    }
//
//    val processed = if(CONFIG.ner){
//      input.map{
//        case l:String =>
//          try{
//            l.split(Sep,2) match {
//              case Array(id, t) =>
//                  val ret = segmenter.segment(t)
//                  id+Sep+ret.map{case(w,n)=>(n.replace("nrt","nr"),w)}.groupBy(_._1).map{case(n,arr)=> s"$n:${arr.map(_._2).distinct.mkString(",")}"}.mkString(";    ")
//            }
//          }catch{
//            case e:MatchError => {
//              val ret = segmenter.segment(l)
//              ErrID.toString + Sep +ret.map{case(w,n)=>(n.replace("nrt","nr"),w)}.groupBy(_._1).map{case(n,arr)=> s"$n:${arr.map(_._2).distinct.mkString(",")}"}.mkString(";    ")
//            }
//          }
//      }
//    } else {
//      input.map{
//        case l:String =>
//          try{
//            l.split(Sep,2) match {
//              case Array(id, t) =>
//                  id+Sep+segmenter.segmentToString(t)
//            }
//          }catch{
//            case e:MatchError => {
//              ErrID.toString + Sep +segmenter.segmentToString(l)
//            }
//          }
//      }
//    }
//
//    processed.saveAsTextFile(CONFIG.output)
//
//    val ab = ArrayBuffer
//  }
//
//}
