package grimoire.util.deserializer

/**
  * Created by caphael on 2016/12/21.
  */
abstract class DataDeserializer[T,U] extends Serializable{
  def deserialize(dat:T):U
}
