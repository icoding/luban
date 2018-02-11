package grimtest.test

import grimoire.configuration.param.{Param, ParamValidators}
import grimoire.spark.transform.rdd.RDDLineSplitSpell
import play.api.libs.json.{JsPath, Json, Format}


/**
  * Created by caphael on 2017/2/16.
  */
object FunctionalTest {

  import play.api.libs.functional.syntax._
  import play.api.libs.functional.{FunctionalCanBuild,Functor, ~}
  import grimoire.configuration.param._


//  class MClass[T]{
//    var value:T = null.asInstanceOf[T]
//    def set(v:T):this.type = {
//      value=v
//      this
//    }
//    def get()= value
//  }
//
//  implicit val fcb = new FunctionalCanBuild[MClass] {
//    override def apply[A, B](ma: MClass[A], mb: MClass[B]): MClass[~[A, B]] = new MClass[A ~ B]().set(new ~(ma.get,mb.get))
//  }
//
//  implicit val fu = new Functor[MClass]{
//    override def fmap[A, B](m: MClass[A], f: (A) => B): MClass[B] =  new MClass().set(f(m.get))
//  }
//
//  val a = new MClass[Int].set(1)
//  val b = new MClass[String].set("bbb")
//
//  (a and b)((a:Int,b:String) => s"a:${a},b:${b}").get()


  val p1 = new Param[String]("p1","p1",ParamValidators.alwaysTrue){
    override val format: Format[Option[String]] = (JsPath \ name).formatNullable[String]
  }

  val p2 = new Param[Int]("p2","p2",ParamValidators.alwaysTrue){
    override val format: Format[Option[Int]] = (JsPath \ name).formatNullable[Int]
  }

//  (p1 and p2)((s:String,i:Int)=>{s"{p1:${s};p2:${i}"})
//  Json.parse("""{"p1":"xxx","p2":999}""")

}


