package util

import java.util.Date
import java.text.SimpleDateFormat
import java.util.Calendar

//* 日期时间工具类
object DateUtils {
  var date = new Date()
  val TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd")
  /**
    * 判断一个时间是否在另一个时间之前
    * @param time1 第一个时间
    * @param time2 第二个时间
    * @return 判断结果
    */
  def after(time1:String,time2:String):Boolean={
    try {
      var dateTime1 = TIME_FORMAT.parse(time1)
      var dateTime2 = TIME_FORMAT.parse(time2)
      if (dateTime1.after(dateTime2)){
        return true
      }
    }catch {
      case e:Throwable => e.printStackTrace()
    }
    false
  }
  /**
    * 计算时间差值（单位为秒）
    * @param time1 时间1
    * @param time2 时间2
    * @return 差值
    */
  def minus(time1:String,time2:String):Integer={
    try {
      var datetime1 = TIME_FORMAT.parse(time1)
      var datetime2 = TIME_FORMAT.parse(time2)

      var millisecond = datetime1.getTime - datetime2.getTime

      return Integer.valueOf(String.valueOf(millisecond / 1000));
    } catch {
      case e:Throwable=>e.printStackTrace()
    }
    0
  }
  /**
    * 获取年月日和小时
    * @param datetime 时间（yyyy-MM-dd HH:mm:ss）
    * @return 结果
    */
  def getDateHour(datetime:String):String={
    var date = datetime.split(" ")(0)
    var hourMinuteSecond = datetime.split(" ")(1)
    var hour = hourMinuteSecond.split(":")(0)
    f"${date}_$hour"
  }
  def getTodayDate:String={
    DATE_FORMAT.format(new Date())
  }
  /**
    * 获取昨天的日期（yyyy-MM-dd）
    * @return 昨天的日期
    */
  def getYesterdayDate:String={
    val cal = Calendar.getInstance()
    cal.setTime(new Date())
    cal.add(Calendar.DAY_OF_YEAR,-1)
    var date = cal.getTime
    DATE_FORMAT.format(date)
  }
  /**
    * 格式化日期（yyyy-MM-dd）
    * @param date Date对象
    * @return 格式化后的日期
    */
  def formatDate(date:Date): String = DATE_FORMAT.format(date)
  /**
    * 格式化时间（yyyy-MM-dd HH:mm:ss）
    * @param date Date对象
    * @return 格式化后的时间
    */
  def formatTime(date:Date):String = TIME_FORMAT.format(date)
}
