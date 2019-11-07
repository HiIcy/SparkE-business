package domain
import lombok.{Data, Getter, Setter}
import scala.beans.BeanProperty

@transient
class SessionDetail {
  @BeanProperty
  var taskid = 0L
  @BeanProperty
  var userid = 0L
  @BeanProperty
  var sessionid: String = _
  @BeanProperty
  var pageid = 0L
  @BeanProperty
  var actionTime:String = _
  @BeanProperty
  var searchKeyword:String =_
  @BeanProperty
  var clickCategoryId = 0L
  @BeanProperty
  var clickProductId = 0L
  @BeanProperty
  var orderCategoryIds:String =_
  @BeanProperty
  var orderProductIds:String =_
  @BeanProperty
  var payCategoryIds:String =_
  @BeanProperty
  var payProductIds:String =_
}
