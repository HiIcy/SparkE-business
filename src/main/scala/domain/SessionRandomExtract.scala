package domain
import scala.beans.BeanProperty

@transient
class SessionRandomExtract {
  @BeanProperty
  var taskid: Long = _
  @BeanProperty
  var sessionid: String = _
  @BeanProperty
  var startTime: String = _
  @BeanProperty
  var searchKeywords: String = _
  @BeanProperty
  var clickCategoryIds: String = _

}
