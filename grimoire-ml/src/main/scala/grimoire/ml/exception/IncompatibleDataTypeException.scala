package grimoire.ml.exception

/**
  * Created by caphael on 2017/3/23.
  */
class IncompatibleDataTypeException(typeName:String,referName:String)
  extends Exception(s"Incompatible data type ${typeName} referred by ${referName}.")

object IncompatibleDataTypeException{
  def apply(typeName: String, referName: String): IncompatibleDataTypeException = new IncompatibleDataTypeException(typeName, referName)
}