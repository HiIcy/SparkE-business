package dao.factory

import dao.{ISessionAggrStatDAO, ISessionDetailDAO, ISessionRandomExtractDAO, ITaskDAO}
import impl.{SessionAggrStatDAOImpl, SessionRandomExtractDAOImpl, TaskDAOImpl,SessionDetailDAOImpl}

object DAOFactory {
  def getTaskDAO:ITaskDAO=new TaskDAOImpl()
  def getSessionAggrStatDAO:ISessionAggrStatDAO=new SessionAggrStatDAOImpl()
  def getSessionRandomExtractDAO:ISessionRandomExtractDAO = new SessionRandomExtractDAOImpl()
  def getSessionDetailDAO:ISessionDetailDAO = new SessionDetailDAOImpl
}
