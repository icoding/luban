package grimoire.streaming.source

import grimoire.logging.Logging
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by caphael on 2017/4/10.
  */
abstract class StreamSource[T] extends Logging{
  def stream:DStream[T]
}
