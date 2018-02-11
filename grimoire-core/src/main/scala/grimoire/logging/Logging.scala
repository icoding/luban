package grimoire.logging

import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by caphael on 2016/12/7.
  */
trait Logging {
  var isInitialed = false
  var _log:Logger = null

  def logName = this.getClass.getSimpleName

  def logger:Logger = {
    if (! isInitialed) {
      _log = LoggerFactory.getLogger(logName)
    }
    _log
  }
}
