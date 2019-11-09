package test

import dao.factory.DAOFactory

/**
  * 任务管理DAO测试类
  */
object TaskDAOTest {
  def main(args: Array[String]): Unit = {
    var taskDao = DAOFactory.getTaskDAO
    var task = taskDao.findById(1)
    println(task.getTaskName)

  }
}
