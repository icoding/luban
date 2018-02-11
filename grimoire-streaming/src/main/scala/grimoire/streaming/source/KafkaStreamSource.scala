//package grimoire.streaming.source
//import org.apache.spark.streaming.dstream.DStream
//
///**
//  * Created by caphael on 2017/4/10.
//  */
//class KafkaStreamSource extends StreamSource[String]{
//  override def stream: DStream[String] = {
//    KafkaUtils.createStream(ssc,zkQuorum,groupid,topics).map(_._2)
//  }
//}
