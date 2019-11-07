package impl

import dao.ISessionRandomExtractDAO
import domain.SessionRandomExtract
import jdbc.JDBCHelper

class SessionRandomExtractDAOImpl extends ISessionRandomExtractDAO {
  override def insert(sessionRandomExtract: SessionRandomExtract): Unit = {
    var sql = "insert into session_random_extract values(?,?,?,?,?)"

    var params = Array[Any](sessionRandomExtract.getTaskid,
      sessionRandomExtract.getSessionid,
      sessionRandomExtract.getStartTime,
      sessionRandomExtract.getSearchKeywords,
      sessionRandomExtract.getClickCategoryIds)

    JDBCHelper.executeUpdate(sql, params)

  }
}
