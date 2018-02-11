package grimoire.common.transform

/**
  * Created by caphael on 2017/3/22.
  */
class LineSplitSpell extends LineSplitLike[String,Seq[String]]{
  override def transformImpl(dat: String): Seq[String] = {
    val ret:Seq[String] = if ($(numField) == 0 ){
      dat.split($(separator))
    }else{
      dat.split($(separator),$(numField))
    }

    if ($(selectIndexes).size == 0){
      ret
    }else{
      try{
        $(selectIndexes).map(ret(_))
      }catch {
        case e:IndexOutOfBoundsException =>
          logger.error("Index of selected field was beyond the number of fields!")
          throw e
      }
    }
  }

}
