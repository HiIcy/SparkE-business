package domain
import scala.beans.BeanProperty
import java.io.Serializable

@transient
class Task {
  private val serialVersionUID = -9205169808581154064L
  @BeanProperty
  var taskid:Long = 0
  @BeanProperty//看到了python的影子
  var taskName:String = "taskname"
  @BeanProperty
  var createTime:String ="createTime"
  @BeanProperty
  var startTime:String ="startTime"
  @BeanProperty
  var finishTime:String = "finishTime"
  @BeanProperty
  var taskType:String ="taskType"
  @BeanProperty
  var taskStatus:String = "taskStatus"
  @BeanProperty
  var taskParam:String ="taskParam"

}
