package util

import com.alibaba.fastjson.JSONObject


object ParamUtils {
  /**
  * 从命令行参数中提取任务id
    * @param args 命令行参数
  * @return 任务id
  */
  def getTaskIdFromArgs(args:Array[String]):Option[Long]={ //REW:
    try {
      if(args != null && args.length>0){
        return Some(args(0).toLong)
      }
    }catch {
      case e:Throwable => None
    }
    None
  }
  /**
    * 从JSON对象中提取参数
    * @param jsonObject JSON对象
    * @return 参数
    */
  def getParam(jsonObject:JSONObject,field:String):Option[String]={
    var jsonValue = jsonObject.getString(field)
    if (jsonValue != null){
      return Some(jsonValue)
    }
//    if (jsonArray!=null && jsonArray.size() > 0){
//      return Some(jsonArray.getString(0))
//    }
    None
  }
}
