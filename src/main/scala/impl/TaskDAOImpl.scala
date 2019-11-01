package impl
import java.sql.ResultSet
import dao.ITaskDAO
import domain.Task
import jdbc.JDBCHelper

/**
  * 任务管理DAO实现类
  * @author Erik
  *
  */

class TaskDAOImpl extends ITaskDAO {
  override def findById(taskid: Long): Task = {
    val task = new Task()
    val sql = "select * from task where task_id=?"
    var params = Seq(taskid)
    JDBCHelper.executeQuery(sql,params,new JDBCHelper.QueryCallback(){
      override def process(rs: ResultSet): Unit = {
        if(rs.next()){
          var taskid = rs.getLong(1)
          var taskName = rs.getString(2)
          var createTime = rs.getString(3)
          var startTime = rs.getString(4)
          var finishTime = rs.getString(5)
          var taskType = rs.getString(6)
          var taskStatus = rs.getString(7)
          var taskParam = rs.getString(8)
//          println(f"taskParam: $taskParam")
          task.setTaskid(taskid)
          task.setTaskName(taskName)
          task.setCreateTime(createTime)
          task.setStartTime(startTime)
          task.setFinishTime(finishTime)
          task.setTaskType(taskType)
          task.setTaskStatus(taskStatus)
          task.setTaskParam(taskParam)
        }
      }
    })
    task
  }
}
