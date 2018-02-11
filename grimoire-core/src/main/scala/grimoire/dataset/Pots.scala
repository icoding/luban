package grimoire.dataset
/**
  * Created by caphael on 2017/2/10.
  */
import grimoire.Implicits._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

case class Pots1[+T : TypeTag](e:Pot[T])
  extends Pot[(T)](e.depends){

  override def toString() = e.toString()

  override def conjureImpl: (T) = e.conjure

  def :+[U : TypeTag](pot:Pot[U]):Pots2[T,U] = Pots2(e,pot)
}

case class Pots2[+T1 : TypeTag,+T2 : TypeTag](_1:Pot[T1], _2:Pot[T2])
  extends Pot[T1&T2](_1.depends ++ _2.depends){

  override def toString() =  Seq(_1,_2).mkString("&")
  def swap: Pots2[T2,T1] = Pots2(_2, _1)

  def :+[U : TypeTag](pot:Pot[U]):Pots3[T1,T2,U] = Pots3(_1,_2,pot)
  override def conjureImpl: T1 & T2 = _1.conjure & _2.conjure
}

case class Pots3[+T1 : TypeTag,+T2 : TypeTag,+T3 : TypeTag](_1:Pot[T1], _2:Pot[T2], _3:Pot[T3])
  extends Pot[T1&T2&T3](_1.depends ++ _2.depends ++ _3.depends){
  override def conjureImpl: T1&T2&T3 = _1.conjure & _2.conjure & _3.conjure

  override def toString() = Seq(_1,_2,_3).mkString("&")

  def :+[U : TypeTag](pot:Pot[U]):Pots4[T1,T2,T3,U] = Pots4(_1,_2,_3,pot)

}

case class Pots4[+T1 : TypeTag,+T2 : TypeTag,+T3 : TypeTag,+T4 : TypeTag](_1:Pot[T1], _2:Pot[T2], _3:Pot[T3], _4:Pot[T4])
  extends Pot[T1&T2&T3&T4](_1.depends ++ _2.depends ++ _3.depends ++ _4.depends){
  override def conjureImpl: T1&T2&T3&T4 = _1.conjure & _2.conjure & _3.conjure & _4.conjure

  override def toString() = Seq(_1,_2,_3,_4).mkString("&")

}