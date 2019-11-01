package util

object ValidUtils {
  /**
    * 校验数据中的指定字段，是否在指定范围内
    * @param data 数据
    * @param dataField 数据字段
    * @param parameter 参数
    * @param startParamField 起始参数字段
    * @param endParamField 结束参数字段
    * @return 校验结果
    */
  def between(data:String,dataField:String,parameter:String,
        startParamField:String,endParamField:String):Boolean={
    var startParamFieldStr = StringUtils.getFieldFromConcatString(
      parameter,"\\|",startParamField
    ) match {
      case Some(x)=>x
      case None => null
    }

    var endParamFieldStr = StringUtils.getFieldFromConcatString(
      parameter, "\\|", endParamField) match {
      case Some(x) => x
      case None => null
    }
    if ( startParamFieldStr == null || endParamFieldStr == null) return true
    var startParamFieldValue = startParamFieldStr.toInt
    var endParamFieldValue = endParamFieldStr.toInt
    var dataFieldStr = StringUtils.getFieldFromConcatString(
      data, "\\|", dataField) match {
      case Some(x) => x
      case None => null
    }
    if (dataFieldStr != null){
      var dataFieldValue = dataFieldStr.toInt
      if(dataFieldValue >= startParamFieldValue &&
        dataFieldValue <= endParamFieldValue){
        return true
      }else{
        return false
      }
    }
    true
  }
  /**
    * 校验数据中的指定字段，是否有值与参数字段的值相同
    * @param data 数据
    * @param dataField 数据字段
    * @param parameter 参数
    * @param paramField 参数字段
    * @return 校验结果
    */
  def in(data:String,  dataField:String,
    parameter:String , paramField:String ):Boolean= {
    var paramFieldValue = StringUtils.getFieldFromConcatString(
      parameter, "\\|", paramField) match {
      case Some(x) => x
      case None => null
    }
    if(paramFieldValue == null) {
      return true
    }
    var paramFieldValueSplited = paramFieldValue.split(",")

    var dataFieldValue = StringUtils.getFieldFromConcatString(
      data, "\\|", dataField) match {
      case Some(x) => x
      case None => null
    }
    if(dataFieldValue != null) {
      var dataFieldValueSplited = dataFieldValue.split(",")

      for(singleDataFieldValue <- dataFieldValueSplited) {
        for(singleParamFieldValue <- paramFieldValueSplited) {
          if(singleDataFieldValue.equals(singleParamFieldValue)) {
            return true
          }
        }
      }
    }
    false
  }
  /**
    * 校验数据中的指定字段，是否在指定范围内
    * @param data 数据
    * @param dataField 数据字段
    * @param parameter 参数
    * @param paramField 参数字段
    * @return 校验结果
    */
  def equal(data:String,dataField:String,parameter:String,
            paramField:String):Boolean={
    var paramFieldValue = StringUtils.getFieldFromConcatString(
      parameter,"\\|",paramField) match {
      case Some(x) => x
      case None => null
    }
    if (paramFieldValue == null){
      return true
    }
    var dataFieldValue = StringUtils.getFieldFromConcatString(
      data, "\\|", dataField) match {
      case Some(x) => x
      case None => null
    }
    if (dataFieldValue != null) if (dataFieldValue.equals(paramFieldValue)) return true
    false
    }
}
