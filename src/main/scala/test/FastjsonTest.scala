import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject
package test
{


  object FastjsonTest {
  def main(args: Array[String]): Unit = {
    var json = f"[{'学生':'张三','班级':'一班','年级':'大一','科目':'高数','成绩':90}," +
    "{'学生':'李四','班级':'二班','年级':'大一','科目':'高数','成绩':80}]"
    val jsonArray = JSON.parseArray(json)
    val jsonObject = jsonArray.get(0)
    println(jsonObject)
  }
}
}
