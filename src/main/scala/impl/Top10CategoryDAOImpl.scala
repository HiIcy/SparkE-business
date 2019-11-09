package impl

import dao.ITop10CategoryDAO
import domain.Top10Category
import jdbc.JDBCHelper

class Top10CategoryDAOImpl extends ITop10CategoryDAO{
    override def insert(top10Category: Top10Category):Unit={
        val sql = "insert into top10_category value(?,?,?,?,?)"
        val params = Array[Any](top10Category.taskid,
            top10Category.categoryid,
            top10Category.clickCount,
            top10Category.orderCount,
            top10Category.payCount)
        JDBCHelper.executeUpdate(sql, params)
    }
}
