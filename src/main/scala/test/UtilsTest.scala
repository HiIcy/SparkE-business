package test
import util.DateUtils
import util.ParamUtils
import com.alibaba.fastjson.JSON
object UtilsTest {
  def main(args: Array[String]): Unit = {
    testDate()
  }
  def testDate():Unit={
    var tim1 = "2019-05-10 05:30:30"
    var tim2 = "2019-06-10 05:30:30"
    var j = "{'startDate':'2019-07-18','endDate':'2019-07-28','sex':'male','startAge':'12','endAge':'30'}"
    var js = JSON.parseObject(j)
    ParamUtils.getParam(js,"endDate")
//    println(DateUtils.after(tim2,tim1))
  }

}
