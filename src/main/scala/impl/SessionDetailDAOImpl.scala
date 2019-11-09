package impl
import dao.ISessionDetailDAO
import domain.SessionDetail
import jdbc.JDBCHelper


class SessionDetailDAOImpl extends ISessionDetailDAO {
  override def insert(sessionDetail: SessionDetail): Unit = {
    val sql = "insert into session_detail value(?,?,?,?,?,?,?,?,?,?,?,?)"
    val params = Array[Any](sessionDetail.getTaskid,
                          sessionDetail.getUserid,
                          sessionDetail.getSessionid,
                          sessionDetail.getPageid,
                          sessionDetail.getActionTime,
                          sessionDetail.getSearchKeyword,
                          sessionDetail.getClickCategoryId,
                          sessionDetail.getClickProductId,
                          sessionDetail.getOrderCategoryIds,
                          sessionDetail.getOrderProductIds,
                          sessionDetail.getPayCategoryIds,
                          sessionDetail.getPayProductIds)
      JDBCHelper.executeUpdate(sql, params)
  }
}
