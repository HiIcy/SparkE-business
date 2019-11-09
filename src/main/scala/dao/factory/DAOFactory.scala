package dao.factory

import dao._
import impl._

object DAOFactory {
  def getTaskDAO:ITaskDAO=new TaskDAOImpl()
  def getSessionAggrStatDAO:ISessionAggrStatDAO=new SessionAggrStatDAOImpl()
  def getSessionRandomExtractDAO:ISessionRandomExtractDAO = new SessionRandomExtractDAOImpl()
  def getSessionDetailDAO:ISessionDetailDAO = new SessionDetailDAOImpl
  def getTop10CategoryDAO:ITop10CategoryDAO = new Top10CategoryDAOImpl
  def getTop10SessionDAO:ITop10SessionDAO = new Top10SessionDAOImpl
}
