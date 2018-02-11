package grimtest.test

/**
  * Created by caphael on 2017/6/15.
  */
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
object DualTest extends App{

  case class Dual[A1:TypeTag,A2:TypeTag](dat:Any)(implicit classA1:ClassTag[A1],classA2:ClassTag[A2]){
    val t = dat match {
      case d:A1 => "A1"
      case d:A2 => "A2"
    }
  }
}
