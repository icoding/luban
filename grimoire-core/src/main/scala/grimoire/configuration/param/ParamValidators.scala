package grimoire.configuration.param

import scala.reflect.ClassTag

/**
  * Created by caphael on 2017/2/14.
  */
object ParamValidators {

  def alwaysTrue[T:ClassTag]:T => Boolean = (value:T) => true

  def notNull[T:ClassTag]:T => Boolean = (value:T) => value != null

  def gt[T:ClassTag](d:Double):T=>Boolean = (value:T) => getDouble(value) > d

  def ge[T:ClassTag](d:Double):T=>Boolean = (value:T) => getDouble(value) >= d

  def inArray[T](allowed: Seq[T]): T => Boolean = { (value: T) =>
    allowed.contains(value)
  }

  def inRange(
                  lowerBound: Double,
                  upperBound: Double,
                  lowerInclusive: Boolean,
                  upperInclusive: Boolean): Double => Boolean = { (value: Double) =>
    val x: Double = value
    val lowerValid = if (lowerInclusive) x >= lowerBound else x > lowerBound
    val upperValid = if (upperInclusive) x <= upperBound else x < upperBound
    lowerValid && upperValid
  }

  def getDouble[T:ClassTag](v:T):Double = {
    v match{
      case i:Int => i.toDouble
      case d:Double => d
      case s:String => s.toDouble
      case l:Long => l.toDouble
      case x:Any => throw new Exception(s"Can not convert value (${x}) to Double!")
    }
  }
}
