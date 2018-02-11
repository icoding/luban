package grimoire.util.json.reader

/**
  * Created by caphael on 2017/3/21.
  */

import grimoire.util.keeper.MapKeeper
import play.api.libs.json.JsValue

import scala.collection.mutable.{Map => MMap}

object JsonReaders extends MapKeeper[String,AbstractJsonReader[_]]{

  val internalMap = MMap[String,AbstractJsonReader[_]]()

  override def toMap(): MMap[String, AbstractJsonReader[_]] = internalMap


  def addReader(reader:AbstractJsonReader[_]):this.type = {
    toMap() += reader.mapEntry()
    this
  }

  val stringReader = new AbstractJsonReader[String]("string") {
    override def read(json: JsValue, name: String): String =
      (json \ name).as[String]
  }

  val intReader = new AbstractJsonReader[Int]("integer") {
    override def read(json: JsValue, name: String): Int =
      (json \ name).as[Int]
  }

  val doubleReader = new AbstractJsonReader[Double]("double") {
    override def read(json: JsValue, name: String): Double =
      (json \ name).as[Double]
  }

  val longReader = new AbstractJsonReader[Long]("long") {
    override def read(json: JsValue, name: String): Long =
      (json \ name).as[Long]
  }

  val seqStringReader = new AbstractJsonReader[Seq[String]]("seq[string]") {
    override def read(json: JsValue, name: String): Seq[String] =
      (json \ name).as[Seq[String]]
  }

  val seqStringDouble = new AbstractJsonReader[Seq[Double]]("seq[double]") {
    override def read(json: JsValue, name: String): Seq[Double] =
      (json \ name).as[Seq[Double]]
  }


}
