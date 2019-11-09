package domain

/**
  * top10活跃session
  * @author Erik
  *
  */
class Top10Session extends Serializable {
    private val serialVersionUID = 215920520401L
    private[this] var _taskid: Long = _
    private[this] var _clickCount: Long = _
    private[this] var _categoryid: Long = _
    private[this] var _sessionid: String = _

    def taskid: Long = _taskid

    def taskid_=(value: Long): Unit = {
      _taskid = value
    }

    def categoryid: Long = _categoryid

    def categoryid_=(value: Long): Unit = {
      _categoryid = value
    }

    def sessionid: String = _sessionid

    def sessionid_=(value: String): Unit = {
      _sessionid = value
    }

    def clickCount: Long = _clickCount

    def clickCount_=(value: Long): Unit = {
      _clickCount = value
    }

}
