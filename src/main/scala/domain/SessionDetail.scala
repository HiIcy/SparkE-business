package domain
import lombok.{Data, Getter, Setter}
import scala.beans.BeanProperty

@transient
class SessionDetail {
  @BeanProperty
  var taskid:Long = _
  @BeanProperty
  var userid:Long = _
  @BeanProperty
  var sessionid: String = _
  @BeanProperty
  var pageid:Long = _
  @BeanProperty
  var actionTime:String = _
  @BeanProperty
  var searchKeyword:String =_
  @BeanProperty
  var clickCategoryId:Long = _
  @BeanProperty
  var clickProductId:Long = _
  @BeanProperty
  var orderCategoryIds:String =_
  @BeanProperty
  var orderProductIds:String =_
  @BeanProperty
  var payCategoryIds:String =_
  @BeanProperty
  var payProductIds:String =_
}
