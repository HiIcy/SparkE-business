package test

import jdbc.JDBCHelper

object JDBCHelperTest {
  def main(args: Array[String]): Unit = {
//    JDBCHelper.executeUpdate("insert into city(Name,CountryCode,District,Population) values(?,?,?,?)",
//      Seq("ldw", "CHN", ",zi", 1))
    // REW:很强的一套组合拳
    /*
    var testCity:Map[String,Any] = Map()
    JDBCHelper.executeQuery("select * from city where id=?",Seq(5),new JDBCHelper.QueryCallback(){
      override def process(rs: ResultSet): Unit = {
        if(rs.next()){
          var name = rs.getString(1)
          testCity += "name"->name
        }
      }
    })
    println(testCity.mkString(","))
    */
    var sql = "insert into city(Name,CountryCode,District,Population) values(?,?,?,?)"
    var paramsList:Array[Seq[Any]] = new Array[Seq[Any]](2)
    paramsList(0)=Seq("hed","CHN","32",5)
    paramsList(1)=Seq("vidow","CHN","32",2114)
    JDBCHelper.executeBatch(sql,paramsList)
  }
}
