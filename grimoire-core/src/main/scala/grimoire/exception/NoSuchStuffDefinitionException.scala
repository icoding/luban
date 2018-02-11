package grimoire.exception

/**
  * Created by caphael on 2017/2/21.
  */
class NoSuchStuffDefinitionException(uid:String) extends Exception(s"No definition of UID:${uid}")
