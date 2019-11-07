package dao

import domain.SessionAggrStat

trait ISessionAggrStatDAO {
  /**
    * session聚合统计模块DAO接口
    * @author Erik
    *
    */
    def insert(sessionAggrStat:SessionAggrStat)
  }

