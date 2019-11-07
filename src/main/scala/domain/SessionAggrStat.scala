package domain
import scala.beans.BeanProperty
import lombok.{Getter, Setter}

/**
  * session聚合统计
  */
@transient
class SessionAggrStat {
  @BeanProperty
  var taskid: Long = _
  @BeanProperty
  var session_count: Long = _

  @BeanProperty
  var visit_length_1s_3s_ratio: Double = _
  @BeanProperty
  var visit_length_4s_6s_ratio: Double = _
  @BeanProperty
  var visit_length_7s_9s_ratio: Double = _
  @BeanProperty
  var visit_length_10s_30s_ratio: Double = _
  @BeanProperty
  var visit_length_30s_60s_ratio: Double = _
  @BeanProperty
  var visit_length_1m_3m_ratio: Double = _
  @BeanProperty
  var visit_length_3m_10m_ratio: Double = _
  @BeanProperty
  var visit_length_10m_30m_ratio: Double = _
  @BeanProperty
  var visit_length_30m_ratio: Double = _
  @BeanProperty
  var step_length_1_3_ratio: Double = _
  @BeanProperty
  var step_length_4_6_ratio: Double = _
  @BeanProperty
  var step_length_7_9_ratio: Double = _
  @BeanProperty
  var step_length_10_30_ratio: Double = _
  @BeanProperty
  var step_length_30_60_ratio: Double = _
  @BeanProperty
  var step_length_60_ratio: Double = _


}

