package dao

import domain.SessionAggrStat

trait ISessionAggrStatDAO {
  /**
    * session聚合统计模块DAO接口
    *
    */
    def insert(sessionAggrStat:SessionAggrStat)
  }

