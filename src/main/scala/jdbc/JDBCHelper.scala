import java.sql.{Connection, DriverManager, ResultSet,
      PreparedStatement,SQLException,Statement}
import scala.collection.mutable.ArrayBuffer
import constant.Constants
import conf.ConfigurationManager
package jdbc{
object JDBCHelper {
  //第一步：在静态代码块中，直接搭载数据库驱动
  //加载驱动，不要硬编码
  //在my.properties中添加
  //djbc.driver=com.mysql.jdbc.Driver
  //在Constants.java中添加
  //String JDBC_DRIVER = "jdbc.driver";
  //FIXME: 如何修改
  try {
    val driver = ConfigurationManager.getProperty(Constants.JDBC_DRIVER)
    Class.forName(driver)
  }catch {
    case e:Exception => e.printStackTrace()
  }
//  /第二步，实现JDBCHelper的单例化
  //因为它的内部要封装一个简单的内部的数据库连接池
  //为了保证数据库连接池有且仅有一份，所以就通过单例的方式
  //保证JDBCHelper只有一个实例，实例中只有一份数据库连接池
  //scala object 天然单例

  //连接池
  private var datasource:List[Connection] = List()

  /**
    * 第三步：实现单例的过程中，创建唯一的数据库连接池
    * 私有化构造方法
    * JDBCHelper在整个程序运行生命周期中，只会创建一次实例
    * 在这一次创建实例的过程中，就会调用JDBCHelper（）的方法
    * 此时，就可以在构造方法中，去创建自己唯一的一个数据库连接池
    */
  // scala 私有初始化不好实现
    var datasourcesize: Int = ConfigurationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE)
    for(i <- 0 until datasourcesize){
      var url = ConfigurationManager.getProperty(Constants.JDBC_URL)
      var user = ConfigurationManager.getProperty(Constants.JDBC_USER)
      var password =ConfigurationManager.getProperty(Constants.JDBC_PASSWORD)
      try {
        var conn = DriverManager.getConnection(url, user, password)
        datasource=datasource.+:(conn)
      } catch {
        case e:SQLException => e.printStackTrace()
      }
    }

  /**
    * 第四步，提供获取数据库连接的方法
    * 有可能，获取的时候连接池已经用光了，暂时获取不到数据库连接
    * 所以要编写一个简单的等待机制，等待获取到数据库连接
    *
    * synchronized设置多线程并发访问限制
    */
  def getConnection:Connection=this.synchronized {
    try{
      while (datasource.isEmpty){
        Thread.sleep(10)
      }
    }catch {
      case e:InterruptedException => e.printStackTrace()
    }
    var head = datasource.head
    datasource = datasource.drop(1)
    head
  }

  /**
    * 第五步：开发增删改查的方法
    * 1.执行增删SQL语句的方法
    * 2.执行查询SQL语句的方法
    * 3.批量执行SQL语句的方法
    */

  /**
    * 执行增删改SQL语句
    * @param sql
    * @param params
    * @return 影响的行数
    */
  def executeUpdate(sql:String,params:Seq[Any]):Int={
    var rtn = 0
    var conn = getConnection
    try{
      var pstmt = conn.prepareStatement(sql)
      for(i <- params.indices){
        pstmt.setObject(i+1,params(i))
      }
      rtn = pstmt.executeUpdate
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      if(conn!=null){
        datasource.:+(conn)
      }
    }
    rtn
  }

  //执行查询SQL语句
  def executeQuery(sql:String ,params:Seq[Any],
     callback:QueryCallback) {
    var conn = getConnection
    try {
      var pstmt = conn.prepareStatement(sql)
      for(i <- params.indices) {
        pstmt.setObject(i + 1, params(i))
      }
      var rs = pstmt.executeQuery

      callback.process(rs);
    } catch  {
      case e:Exception => e.printStackTrace()
    } finally {
      if (conn != null) {
        datasource.:+(conn)  //FIXME:把LIst改成其他类型
      }
    }
  }
  class QueryCallback{
    def process(rs:ResultSet):Unit={throw new Exception("error")}
  }
  /**
    * 批量执行SQL语句
    *
    * 批量执行SQL语句，是JDBC中的一个高级功能
    * 默认情况下，每次执行一条SQL语句，就会通过网络连接，想MySQL发送一次请求
    * 但是，如果在短时间内要执行多条结构完全一样的SQL，只是参数不同
    * 虽然使用PreparedStatment这种方式，可以只编译一次SQL，提高性能，
    * 但是，还是对于每次SQL都要向MySQL发送一次网络请求
    *
    * 可以通过批量执行SQL语句的功能优化这个性能
    * 一次性通过PreparedStatement发送多条SQL语句，可以几百几千甚至几万条
    * 执行的时候，也仅仅编译一次就可以
    * 这种批量执行SQL语句的方式，可以大大提升性能
    *
    * @param sql
    * @param paramsList
    * @return每条SQL语句影响的行数
    */
  def executeBatch (sql:String, paramsList:Array[Seq[Any]]):Array[Int]={
    var rtn = Array[Int](0)
    var conn = getConnection
    try {
      //第一步：使用Connection对象，取消自动提交
      conn.setAutoCommit(false)
      var pstmt = conn.prepareStatement(sql)

      //第二步：使用PreparedStatement.addBatch()方法加入批量的SQL参数
      for(params <- paramsList) {
        for (i <- params.indices) {
          pstmt.setObject(i + 1, params(i))
        }
        pstmt.addBatch()

        //第三步：使用PreparedStatement.executeBatch（）方法，执行批量SQL语句
        rtn = pstmt.executeBatch()
      }
      //最后一步，使用Connecion对象，提交批量的SQL语句
      conn.commit()
    } catch  {
      case e:Exception => e.printStackTrace()
    }
    rtn
  }
  }
}
