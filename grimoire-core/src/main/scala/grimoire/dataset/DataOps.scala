package grimoire.dataset

import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.universe._

/**
  * Created by caphael on 2017/3/27.
  */

class AndOps[T:TypeTag](o:T) {
  def &[U:TypeTag](other:U): T & U ={
    new &(o,other)
  }
}

case class &[+A1 :TypeTag,+A2:TypeTag](_1:A1, _2:A2) extends Product2[A1,A2] with Serializable{
  val products: Seq[Any] = {
    val map1 = _1 match {
      case o : &[_,_] => o.products
      case o:Any => Seq(o)
    }
    map1 :+ _2
  }

  def get[T:ClassTag](ct:ClassTag[T]):Seq[T] = {
    getAs[T]
  }

  def getAs[T:ClassTag]():Seq[T] = {
    products.filter{
      case _:T => true
      case _ => false
    }.map(_.asInstanceOf[T])
  }

  def and[T:TypeTag](other:T):A1 & A2 & T = {
    new &(this ,other)
  }

  def &[T:TypeTag](other:T):A1 & A2 & T = new &(this,other)
}