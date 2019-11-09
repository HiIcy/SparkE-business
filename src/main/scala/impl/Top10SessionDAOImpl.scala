package impl

import dao.ITop10SessionDAO
import domain.Top10Session
import jdbc.JDBCHelper

class Top10SessionDAOImpl extends ITop10SessionDAO {
    override def insert(top10Session: Top10Session): Unit = {
        val sql = "insert into top10_session values(?,?,?,?)"
        val params = Array[Any](top10Session.taskid,
            top10Session.categoryid,
            top10Session.sessionid,
            top10Session.clickCount)
        JDBCHelper.executeUpdate(sql, params)

    }
}
