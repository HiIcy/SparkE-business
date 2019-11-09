package domain
import scala.beans.BeanProperty
/*
    top10品类
* */

class Top10Category {
    private[this] var _taskid: Long = _
    private[this] var _categoryid: Long = _
    private[this] var _clickCount: Long = _
    private[this] var _orderCount: Long = _
    private[this] var _payCount: Long = _

    def taskid: Long = _taskid

    def taskid_=(value: Long): Unit = {
      _taskid = value
    }

    def categoryid: Long = _categoryid

    def categoryid_=(value: Long): Unit = {
      _categoryid = value
    }

    def clickCount: Long = _clickCount

    def clickCount_=(value: Long): Unit = {
      _clickCount = value
    }

    def orderCount: Long = _orderCount

    def orderCount_=(value: Long): Unit = {
      _orderCount = value
    }

    def payCount: Long = _payCount

    def payCount_=(value: Long): Unit = {
      _payCount = value
    }

}
