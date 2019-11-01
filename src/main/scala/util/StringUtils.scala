package util
import scala.util.control._

object StringUtils {
//  判断字符串是否为空
  def isEmpty(str:String):Boolean= str==null || "".equals(str)
//  * 判断字符串是否不为空
  def isNotEmpty(str:String):Boolean=str!=null && !"".equals(str)
//  截断字符串两侧的逗号
  def trimComma(str :String):String={
    var strr = ""
    if(str.startsWith(",")){
      strr = str.substring(1)
    }
    if (str.endsWith(",")){
      strr = str.substring(0,str.length()-1)
    }
    strr = str
    strr
  }
//  补全两位数字
  def fulfuill(str:String):String={
    if(str.length() == 2){
       str
    }else{
       "0".concat(str)
    }
  }
  /**
    * 从拼接的字符串中提取字段
    * @param str 字符串
    * @param delimiter 分隔符
    * @param field 字段
    * @return 字段值
    */
  def getFieldFromConcatString(str:String,delimiter:String,field:String):Option[String]={
    var fields = str.split(delimiter)
    for(concatField <- fields){
      var fieldName = concatField.split("=")(0)
      var fieldValue = concatField.split("=")(1)
      if(fieldName.equals(field)){
        Some(fieldValue)
      }
    }
    None
  }
  /**
    * 从拼接的字符串中给字段设置值
    * @param str 字符串
    * @param delimiter 分隔符
    * @param field 字段名
    * @param newFieldValue 新的field值
    * @return 字段值
    */
  def setFieldFromConcatString(str:String,delimiter:String,field:String,
                               newFieldValue:String):String={
    var fields = str.split(delimiter)
    val loop = new Breaks
    loop.breakable {
      for (i <- fields.indices) {
        var fieldName = fields(i).split("=")(0)
        if (fieldName.equals(field)) {
          var concatField = fieldName + "=" + newFieldValue
          fields(i) = concatField
          loop.break()
        }
      }
    }
    var buffer = new StringBuffer("")
    loop.breakable{
      for(i <- fields.indices){
        buffer.append(fields(i))
        if(i<fields.length-1){
          buffer.append("|")
        }
      }
    }
    buffer.toString
  }
}
