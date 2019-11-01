package dao
import domain.Task
import java.io.Serializable

trait ITaskDAO {
  def findById(taskid:Long):Task
}
