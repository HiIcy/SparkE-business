package dao
import domain.SessionRandomExtract
trait ISessionRandomExtractDAO {
  /**
    * 插入session随机抽取
    */
  def insert(sessionRandomExtract: SessionRandomExtract): Unit

}
