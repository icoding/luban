package grimoire.operation.parser

import grimoire.dataset.Pot
import grimoire.transform.Spell

/**
  * Created by caphael on 2017/2/27.
  */
//Result Classes
case class PotResult(val pot:Pot[Any])
case class SpellResult(val spell:Spell[Any,Any])