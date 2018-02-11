package grimoire.job.configuration

import puck.configuration.{Argument, ConfigLike}

/**
  * Created by caphael on 2017/2/28.
  */
trait HasScrollName extends ConfigLike{
  private val scrollName = new Argument[String,ConfigType]("scroll-name","Name of scroll driver","Unnamed")
  def getScrollName() = $(scrollName)

  commandParser.opt[scrollName.ValueType](scrollName.name) abbr("s") action{
    case (x:scrollName.ValueType,c:ConfigType) => scrollName.set(x);c
  } text(scrollName.doc) required()
}

trait HasFilePath extends ConfigLike{
  private val filePath = new Argument[String,ConfigType]("file-path","Path of Json File",null)
  def getFilePath() = $(filePath)

  commandParser.opt[filePath.ValueType](filePath.name) abbr("f") action{
    case (x:filePath.ValueType,c:ConfigType) => filePath.set(x);c
  } text(filePath.doc) required()
}

trait HasPlanFile extends ConfigLike{
  private val planFile = new Argument[String,ConfigType]("plan-file","Path of Plan File",null)
  def getPlanFile() = $(planFile)

  commandParser.opt[planFile.ValueType](planFile.name) abbr("f") action{
    case (x:planFile.ValueType,c:ConfigType) => planFile.set(x);c
  } text(planFile.doc) required()
}