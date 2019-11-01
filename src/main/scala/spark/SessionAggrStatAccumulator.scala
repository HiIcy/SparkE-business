import org.apache.spark.AccumulatorParam
import constant.Constants
import util.StringUtils

/**
  * session聚合统计Accumulator
  * 使用自己定义的一些数据格式，比如String，甚至说，我们可以自己定义model，自己定义的类（必须可序列化）
  * 然后可以基于这种特殊的数据格式实现复杂的分布式计算逻辑
  * 各个task分布式在运行，可以根据自己的需求，task给Accumulator传入不同的值
  * 根据不同的值，去做复杂的逻辑
  *
  * Spark Core使用的高端技术
  * @author Erik
  *
  */
package spark{
  // 自定义累加器
class SessionAggrStatAccumulator extends AccumulatorParam[String] {
  private val serialVersionUID = -2113961376143864034L
  //zero方法，主要用于数据初始化
  //这里就返回一个值，就是初始化中，所有范围区间的数值都是0
  //各个范围区间的统计数量的拼接，还是采用key=value|key=value的连接串格式
  override def zero(initialValue: String): String = {
    Constants.SESSION_COUNT + "=0|"+ Constants.TIME_PERIOD_1s_3s + "=0|"+
      Constants.TIME_PERIOD_4s_6s + "=0|"+ Constants.TIME_PERIOD_7s_9s +
      "=0|"+ Constants.TIME_PERIOD_10s_30s + "=0|"+
      Constants.TIME_PERIOD_30s_60s + "=0|"+
      Constants.TIME_PERIOD_1m_3m + "=0|"+
      Constants.TIME_PERIOD_3m_10m + "=0|"+
      Constants.TIME_PERIOD_10m_30m + "=0|"+
      Constants.TIME_PERIOD_30m + "=0|"+
      Constants.STEP_PERIOD_1_3 + "=0|"+
      Constants.STEP_PERIOD_4_6 + "=0|"+
      Constants.STEP_PERIOD_7_9 + "=0|"+
      Constants.STEP_PERIOD_10_30 + "=0|"+
      Constants.STEP_PERIOD_30_60 + "=0|" +
      Constants.STEP_PERIOD_60 + "=0"
  }

  //addInPlace和addAccumulator可以理解为是一样的
  //这两个方法，主要实现，v1可能就是我们初始化的那个连接串
  //v2就是在遍历session的时候，判断出某个session对应的区间，然后用Constants.TIME_PERIOD_1S_3S
  //所以，我们要做的事情就是在v1中，找到对应的v2对应的value，累加1，然后再更新到连接串里面去
  override def addInPlace(r1: String, r2: String): String = null

  /**
    * session统计计算逻辑
    *
    * @param v1连接串
    * @param v2 范围区间
    * @return更新以后的连接串
    */
  private def add(v1:String,v2:String):String={
    //校验，v1为空的时候 直接返回v2
    if(StringUtils.isEmpty(v1)){
      return v2
    }
    //使用StringUtils工具类，从v1中提取v2对应的值，并累加1
    var oldValue = StringUtils.getFieldFromConcatString(v1,"\\|",v2)
    var newValue = oldValue match {
      case Some(x) => Integer.valueOf(x) + 1
      case None => return v1
    }
    StringUtils.setFieldFromConcatString(v1,"\\|",v2,
      newValue.toString)
  }
}
}
