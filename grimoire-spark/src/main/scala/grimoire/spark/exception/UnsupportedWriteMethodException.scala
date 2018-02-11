package grimoire.spark.exception

import grimoire.configuration.param.WriteMethods

/**
  * Created by caphael on 2017/2/13.
  */
class UnsupportedWriteMethodException(m:WriteMethods.method) extends Exception(s"Unsupported write method: ${m.toString}")

object UnsupportedWriteMethodException{
  def apply(m: WriteMethods.method): UnsupportedWriteMethodException = new UnsupportedWriteMethodException(m)
}
