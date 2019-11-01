package util

import java.math.BigDecimal

object NumberUtils {
  /**
    * 格式化小数
    * @param str 字符串
    * @param scale 四舍五入的位数
    * @return 格式化小数
    */
  def formatDouble(num:Double,scale:Int):Double={
    var bd = new BigDecimal(num)
    bd.setScale(scale,BigDecimal.ROUND_HALF_UP).doubleValue()
  }
}
