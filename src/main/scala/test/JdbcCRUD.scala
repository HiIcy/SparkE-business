package test

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Statement}

//
object JdbcCRUD {
  val driverName = "com.mysql.jdbc.Driver"
  val conn_str = "jdbc:mysql://localhost:3306/world?user=root&password=123456"

  def main(args: Array[String]): Unit = {
    prepareStatement()
//    select()
  }
  def insert():Unit={
    Class.forName(driverName).newInstance()

    //引用JDBC相关的所有接口或者是抽象类的时候，必须是引用java.sql包下的
    //java.sql包下的，才代表了java提供的JDBC接口，只是一套规范
    var conn:Connection=DriverManager.getConnection(conn_str)
    //定义SQL语句执行句柄:Statement对象
    //Statement对象其实就是底层基于Connection数据库连接
    var stmt:Statement =  conn.createStatement()
    try {
      //第一步，加载数据库驱动
      //使用Class.forName()方式加载数据库的驱动类
      //Class.forName()是Java提供的一种基于反射的方式，直接根据类的全限定名（包+类）
      //从类所在的磁盘文件（.class文件）中加载类对应的内容，并创建对应的class对象
//      Class.forName(driverName).newInstance()
      //获取数据库的连接
      //使用DriverManager.getConnection()方法获取针对数据库的连接
      //需要给方法传入三个参数，包括url、user、password
      //其中url就是有特定格式的数据库连接串，包括“主协议：子协议“//主机名：端口号//数据库”
//      conn = DriverManager.getConnection(conn_str)
      //基于数据库连接的Connection对象，创建SQL语句执行句柄，Statement对象
      //Statement对象 ，就是用于基于底层的Connection代表的数据库连接
      //允许我们通过Java程序，通过Statement对象，向MySQL数据库发送SQL语句
      //从而实现通过发送的SQL语句来执行增删改查等逻辑
//      stmt = conn.createStatement()
      //基于Statement对象，来执行insert SQL语句插入一条数据
      //Statement.executeUpdate()方法可以用来执行insert、update、delete语句
      //返回类型是int值，也就是SQL语句影响的行数
      var sql = "insert into city(Name,CountryCode,District,Population) " +
        "values('Mrw','2030','xx',8)"
      var rtn = stmt.executeUpdate(sql)
      println(f"SQL语句影响了【$rtn】行。")
    }
    catch {
      case e:Throwable=>e.printStackTrace()
    } finally {
      try {
        if (stmt != null){
          stmt.close()
        }
        if (conn != null){
          conn.close()
        }
      } catch{
        case e: Throwable =>e.printStackTrace()
      }
    }
  }
  def select():Unit={
    // TODO: 改进try
    Class.forName(driverName).newInstance()
    val conn: Connection = DriverManager.getConnection(conn_str)
    val stmt: Statement = conn.createStatement()
    val sql = "select * from city limit 3 offset 97"
    var rs:ResultSet =stmt.executeQuery(sql)
    try {
      //获取到ResultSet以后，就要对其进行遍历，然后获取查询出来的每一条数据
      while (rs.next()){
        var id =rs.getInt(1)
        var name = rs.getString(2)
        var countrycode = rs.getString(3)
        var district = rs.getString(4)
        var population = rs.getString(5)
        println(f"$name;$countrycode;$district;$population")
      }
    }
    catch {
      case e:Throwable=>e.printStackTrace()
    } finally {
      try {
        if (stmt != null){
          stmt.close()
        }
        if (conn != null){
          conn.close()
        }
      } catch{
        case e: Throwable =>e.printStackTrace()
      }
    }
  }
  /**
    * 使用Statement时，必须在SQL语句中，实际地区嵌入值，容易发生SQL注入
    * 而且性能低下
    *
    * 使用PreparedStatement，就可以解决上述的两个问题
    * 1.SQL主任，使用PreparedStatement时，是可以在SQL语句中，对值所在的位置使用？这种占位符，
    * 实际的值是放在数组中的参数，PreparedStatement会对数值做特殊处理，往往处理后会使恶意注入的SQL代码失效。
    * 2.提升性能，使用P热怕热的Statement后，结构类似的SQL语句会变成一样的，因为值的地方会变成？，
    * 一条SQL语句，在MySQL中只会编译一次，后面的SQL语句过来，就直接拿编译后的执行计划加上不同的参数直接执行，
    * 可以大大提升性能
    */
  def prepareStatement():Unit={

    var conn = DriverManager.getConnection(conn_str)
    Class.forName(driverName).newInstance()
    conn = DriverManager.getConnection(conn_str)
    var sql = "insert into city(Name,CountryCode,District,Population) values(?,?,?,?)"
    var pstmt:PreparedStatement = conn.prepareStatement(sql)
    try {
      //第二个，必须调用PreparedStatement的setX（）系列方法，对指定的占位符赋值
      pstmt.setString(1,"Mru")
      pstmt.setString(2,"CHN")
      pstmt.setString(3,"Mru")
      pstmt.setInt(4,500)
      var rtn = pstmt.executeUpdate
    }catch {
      case e:Throwable=>e.printStackTrace()
    } finally {
      try {
        if (pstmt != null){
          pstmt.close()
        }
        if (conn != null){
          conn.close()
        }
      } catch{
        case e: Throwable =>e.printStackTrace()
      }
    }
  }
}
