package grimoire.transform

/**
  * Created by caphael on 2017/3/22.
  */
trait HasSingleSpellLike[-T,+U]{
  val single:Spell[T,U]
}
