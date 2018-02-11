package grimoire.spark.transform.dataframe

import grimoire.transform.Spell
import org.apache.spark.sql.DataFrame

/**
  * Created by caphael on 2017/1/10.
  */
abstract class DataFrameSpell extends Spell[DataFrame,DataFrame]