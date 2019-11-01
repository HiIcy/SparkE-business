package impl
import dao.ITaskDAO

object DAOFactory {
  def getTaskDAO:ITaskDAO=new TaskDAOImpl()
}
