package grimoire.zeppelin.script

/**
  * Created by caphael on 2017/7/25.
  */
trait ToScript {
  val interpreterName:String

  def interpreterDecl:String = "%"+interpreterName

}

trait ToMDScript extends ToScript{
  override val interpreterName: String = "md"
}

trait ToTableScript extends ToScript{
  override val interpreterName: String = "table"
}

trait ToAngularScript extends ToScript{
  override val interpreterName: String = "angular"
}

