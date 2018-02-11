package grimtest.test

import grimoire.configuration.ConfigLike
import org.apache.hadoop.yarn.util.Apps

/**
  * Created by caphael on 2017/2/14.
  */
object ConfigTest extends Apps{
  import play.api.libs.json._
  import play.api.libs.functional.syntax._
  import grimoire.configuration.param.{HasInputPath, HasOutputPath}

//  case class A() extends ConfigLike with HasInputPath with HasOutputPath
//
//  def init(in:String,out:String): A =
//    new A().setInputPath(in).setOutputPath(out)
//
//  val config:A = A()
//
//  val jstr = """{"inputpath":"aaa","outputpath":"bbb"}"""
//
//  implicit val configReads:Reads[A] = (
//    (JsPath \ "inputpath").read[String] and
//      (JsPath \ "outputpath").read[String]
//   )(init _)
//
//  Json.fromJson[A](Json.parse(jstr))

  val rootReads = new Reads[JsValue] {
    override def reads(json: JsValue): JsResult[JsValue] = JsSuccess(json)
  }
  val aReads =  (JsPath \ "a").read[Int]
  val bReads =  (JsPath \ "b").read[String]

  case class A(a:Int)

  def create(x:Any,a:Int)=A(a)


  implicit val configReads = (rootReads and aReads)(create _)

  val json = Json.parse("""{"a":4}""")

}
