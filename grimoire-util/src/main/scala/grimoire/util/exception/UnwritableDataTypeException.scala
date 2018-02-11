package grimoire.util.exception

/**
  * Created by caphael on 2017/3/23.
  */
class UnwritableDataTypeException(o:Any) extends Exception(s"Unwritable Data Type:${o.getClass.getName}")

object UnwritableDataTypeException{
  def apply(o: Any): UnwritableDataTypeException = new UnwritableDataTypeException(o)
}