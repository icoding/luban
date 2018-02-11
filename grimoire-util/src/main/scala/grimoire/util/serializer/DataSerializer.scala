package grimoire.util.serializer

/**
  * Created by caphael on 2017/3/23.
  */
abstract class DataSerializer[T,U] extends Serializable{

  def serialize(dat:T):U

}
