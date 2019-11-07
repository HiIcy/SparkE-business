package test
import constant.Constants
import org.apache.spark.AccumulatorParam
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import util.StringUtils
class SessionAggrStatAccumulatorTest extends AccumulatorParam[String] {
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
      Constants.STEP_PERIOD_60 + "=0"}

  override def addInPlace(r1: String, r2: String): String = {
    if(StringUtils.isEmpty(r1)){
      return r2
    }
    println(r2)
    //使用StringUtils工具类，从v1中提取v2对应的值，并累加1
    var oldValue = StringUtils.getFieldFromConcatString(r1,"\\|",r2)
    var newValue = oldValue match {
      case Some(x) => Integer.valueOf(x) + 1
      case None =>println("sf")
        return r1
    }
    var res = StringUtils.setFieldFromConcatString(r1,"\\|",r2,
      newValue.toString)
    println(res)
    res
  }
}
object SessionAggrStatAccumulatorTest {
  def main(args: Array[String]): Unit = {
    /**
      * Scala中，自定义Accumulator
      * 使用object，直接定义一个伴生对象即可
      * 需要实现AccumulatorParam接口，并使用[]语法，定义输入输出的数据格式
      */
    // 创建Spark上下文
    val conf = new SparkConf()
      .setAppName("SessionAggrStatAccumulatorTest")
      .setMaster("local")
    val sc = new SparkContext(conf)

    // 使用accumulator()()方法（curry），创建自定义的Accumulator
    val sessionAggrStatAccumulator = sc.accumulator("")(new SessionAggrStatAccumulatorTest())

    // 模拟使用自定义的Accumulator
    val arr = Array(Constants.TIME_PERIOD_1s_3s, Constants.TIME_PERIOD_4s_6s)
    val rdd = sc.parallelize(arr, 1)
    rdd.foreach { sessionAggrStatAccumulator.add }

    println(sessionAggrStatAccumulator.value)
  }
}
