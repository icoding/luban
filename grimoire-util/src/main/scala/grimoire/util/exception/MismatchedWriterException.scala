package grimoire.util.exception

/**
  * Created by caphael on 2017/3/23.
  */
class MismatchedWriterException(writerType:String, actualType:String)
  extends Exception(s"Mismatched writer.Writer type:${writerType} , Actual data type:${actualType}")

object MismatchedWriterException{
  def apply(writerType: String, actualType: String): MismatchedWriterException
  = new MismatchedWriterException(writerType, actualType)
}