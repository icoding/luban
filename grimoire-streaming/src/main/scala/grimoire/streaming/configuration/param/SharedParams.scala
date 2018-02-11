//package grimoire.streaming.configuration.param
//
//import grimoire.configuration.ConfigLike
//import grimoire.configuration.param.Param
//import play.api.data.validation.ValidationError
//import play.api.libs.json._
//
///**
//  * Created by caphael on 2017/4/10.
//  */
//
//trait HasKafkaParams extends ConfigLike{
//  final val zookeeperQuorum = new Param[String](this,"ZooKeeperQuorum","ZooKeeper connection description.") {
//    @transient override private[grimoire] val format = (JsPath \ name).formatNullable[String]
//  }
//
//  def setZooKeeperQuorum(a:Option[String]) = set(zookeeperQuorum,a)
//
//  final val groupid = new Param[String](this,"GroupID","Group Id of a Topic.") {
//    @transient override private[grimoire] val format = (JsPath \ name).formatNullable[String]
//  }
//
//  def setGroupid(a:Option[String]) = set(groupid,a)
//
//  final val topics = new Param[Map[String,Int]](this,"Topics","Topics of Kafka.") {
//    @transient override private[grimoire] val format = new Format[Option[Map[String,Int]]] {
//      override def reads(json: JsValue): JsResult[Option[Map[String, Int]]] = {
//        (json \ name) match {
//          case JsDefined(JsString(s)) => {
//            val ret = s.split(";").map{
//              _.split(":",2) match {case Array(t,i)=>(t,i.toInt)}
//            }.toMap
//            JsSuccess[Option[Map[String,Int]]](Some(ret))
//          }
//          case u:JsUndefined =>
//            JsSuccess[Option[Map[String,Int]]](None)
//          case _ =>
//            JsError(Seq(JsPath() -> Seq(ValidationError("error.expected.jsstring"))))
//        }
//      }
//
//      override def writes(o: Option[Map[String, Int]]): JsValue = {
//        Json.obj(
//          name -> o.map{
//            _.map{
//
//            }
//          }
//        )
//      }
//    }
//  }
//
//}