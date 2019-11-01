package spark
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import conf.ConfigurationManager
import constant.Constants
import impl.DAOFactory
import test.MockData
import util.{ParamUtils, StringUtils, ValidUtils}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject
import groovy.lang.Tuple
import org.apache.spark.rdd.RDD

import scala.Tuple2

object UserVisitSessionAnalyzeSpark {
  Logger.getLogger("org").setLevel(Level.ERROR)
  //  REW 这个参数是web平台传过来的
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","D:\\hiicy\\Download\\Spark大型电商项目实战软件包\\jdk\\hadoop-2.5.0")

    var conf = new SparkConf()
              .setAppName(Constants.SPARK_APP_NAME)
            .setMaster("local")
      .set("spark.sql.broadcastTimeout","36000")
    var sc:SparkContext = new SparkContext(conf)
    var sqlContext= getSQLContext(sc)

    //     生成模拟测试数据
    mockData(sc,sqlContext)
    // 创建需要使用的DAO组件
    val taskDAO =DAOFactory.getTaskDAO
    //那么就首先得查询出来指定的任务，并获取任务的查询参数
    var taskid = Option[Long](1) // ParamUtils.getTaskIdFromArgs(args)
    taskid match {
      case None => System.exit(0)
      case Some(x) => x
    }

    var task = taskDAO.findById(taskid.getOrElse(0))//find的时候就把参数设置了
    var taskParam = JSON.parseObject(task.taskParam) //转成一个json对象
    //如果要进行session粒度的数据聚合，
    //首先要从user_visit_action表中，查询出来指定日期范围内的数据

    var actionRDD = getActionRDDByDateRange(sqlContext,taskParam)
//    println(actionRDD.getClass)
//    var res = actionRDD.collect()
//    res.foreach(println)
//    //聚合
//    //首先，可以将行为数据按照session_id进行groupByKey分组
//    //此时的数据粒度就是session粒度了，然后可以将session粒度的数据与用户信息数据进行join
//    //然后就可以获取到session粒度的数据，同时数据里面还包含了session对应的user信息
////    到这里为止，获取的数据是<sessionid,(sessionid,searchKeywords,
////    clickCategoryIds,age,professional,city,sex)>
    var sessionid2AggrInfoRDD = aggregateBySession(sqlContext,actionRDD)
//    sessionid2AggrInfoRDD.collect()
    //接着，就要针对session粒度的聚合数据，按照使用者指定的筛选参数进行数据过滤
    //相当于我们自己编写的算子，是要访问外面的任务参数对象的
    //匿名内部类（算子函数），访问外部对象，是要给外部对象使用final 我用的val 修饰的
    var filteredSessionid2AggrInfoRDD = filterSession(sessionid2AggrInfoRDD,taskParam)
//    filteredSessionid2AggrInfoRDD.collect()

    sc.stop()
  }
  /**
    * 获取SQLContext
    * 如果在本地测试环境的话，那么生成SQLContext对象
    *如果在生产环境运行的话，那么就生成HiveContext对象
    * @param sc SparkContext
    * @return SQLContext
    */
  def getSQLContext(sc:SparkContext):SQLContext={
    //在my.properties中配置
    //spark.local=true（打包之前改为flase）
    //在ConfigurationManager.java中添加
    //public static Boolean getBoolean(String key) {
    //  String value = getProperty(key)
    //  try {
    //      return Boolean.valueOf(value)
    //  } catch (Exception e) {
    //      e.printStackTrace()
    //  }
    //  return false
    //}
    //在Contants.java中添加
    //String SPARK_LOCAL = "spark.local"
    var local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    if(local) {new SQLContext(sc)}
    else { new HiveContext(sc)}
  }
  /**
    * 生成模拟数据
    * 只有是本地模式，才会生成模拟数据
    * @param sc
    * @param sqlContext
    */
  def mockData(sc:SparkContext,sqlContext:SQLContext):Unit={
    var local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    if(local){
      MockData.mock(sc,sqlContext)
    }
  }
  /**
    * 获取指定日期范围内的用户访问行为数据
    * @param sqlContext SQLContext
    * @param taskParam 任务参数
    * @return 行为数据RDD
    */
  def getActionRDDByDateRange(sqlContext:SQLContext,taskParam:JSONObject):RDD[Row]={
    import sqlContext.implicits._
    //先在Constants.java中添加任务相关的常量
    //String PARAM_START_DATE = "startDate"
    //String PARAM_END_DATE = "endDate"

    var startDate = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE) match {
      case Some(x) => x
      case None =>""
    }
    var endDate = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE) match {
      case Some(x)=>x
      case None => ""
    }

    // 模拟了一张user_visit_action表
    var sql = f"select * from user_visit_action where date>='$startDate'" +
      f" and date<='$endDate'"
//    var sql = f"select date from user_visit_action limit 2"
//    println(sql)
    var actionDF:DataFrame = sqlContext.sql(sql)
//    af.show()
    actionDF.rdd
  }
  /**
    * 对行为数据按sesssion粒度进行聚合
    * @param actionRDD 行为数据RDD
    * @return session粒度聚合数据
    */
  def mapToPair(tuple:(String, Iterable[Row])):Tuple2[Long, String]={
    var sessionid = tuple._1
    var iterator = tuple._2.iterator
    var searchKeywordsBuffer = new StringBuffer("")
    var clickCategoryIdsBuffer = new StringBuffer("")
    var userid:Long = 0L
    while (iterator.hasNext) {
      //提取每个 访问行为的搜索词字段和点击品类字段
      var row = iterator.next()
      if (userid.equals(0L)) {
        userid = row.getLong(1)
      }
      var searchKeyword = row.getString(5)
      var clickCategoryId = row.getLong(6)
      //实际上这里要对数据说明一下
      //并不是每一行访问行为都有searchKeyword和clickCategoryId两个字段的
      //其实，只有搜索行为是有searchKeyword字段的
      //只有点击品类的行为是有clickCaregoryId字段的
      //所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的

      //所以是否将搜索词点击品类id拼接到字符串中去
      //首先要满足不能是null值
      //其次，之前的字符串中还没有搜索词或者点击品类id
      if (StringUtils.isNotEmpty(searchKeyword)) {
        if (!searchKeywordsBuffer.toString.contains(searchKeyword)) {
          searchKeywordsBuffer.append(f"$searchKeyword,")
        }
      }

      if (!clickCategoryIdsBuffer.toString.contains(
        clickCategoryId.toString)) {
        clickCategoryIdsBuffer.append(clickCategoryId + ",")
      }
    }
      var searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString)
      var clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString)
      //返回的数据即是<sessionid, partAggrInfo>
      //但是，这一步聚合后，其实还需要将每一行数据，根对应的用户信息进行聚合
      //问题来了，如果是跟用户信息进行聚合的话，那么key就不应该是sessionid，而应该是userid
      //才能够跟<userid, Row>格式的用户信息进行聚合
      //如果我们这里直接返回<sessionid, partAggrInfo>,还得再做一次mapToPair算子
      //将RDD映射成<userid,partAggrInfo>的格式，那么就多此一举

      //所以，我们这里其实可以直接返回数据格式就是<userid,partAggrInfo>
      //然后在直接将返回的Tuple的key设置成sessionid
      //最后的数据格式，还是<sessionid,fullAggrInfo>
      //聚合数据，用什么样的格式进行拼接？
      //我们这里统一定义，使用key=value|key=vale

      //在Constants.java中定义spark作业相关的常量
      //String FIELD_SESSION_ID = "sessionid"
      //String FIELD_SEARCH_KEYWORDS = "searchKeywords"
      //String FIELD_CLICK_CATEGORY_IDS = "clickCategoryIds"
      var partAggrInfo = f"${Constants.FIELD_SESSION_ID}=$sessionid|" +
        f"${Constants.FIELD_SEARCH_KEYWORDS}=$searchKeywords|" +
        f"${Constants.FIELD_CLICK_CATEGORY_IDS}=$clickCategoryIds"
      var s = new Tuple2[Long,String](userid,partAggrInfo)
      s
  }
  def aggregateBySession(sqlContext:SQLContext,actionRDD:RDD[Row]):RDD[Tuple2[String,String]]={
    //现在actionRDD中的元素是Row，一个Row就是一行用户访问行为记录，比如一次点击或者搜索
    //现在需要将这个Row映射成<sessionid,Row>的格式
    var sessionid2ActionRDD = actionRDD.map(
                      row => (row.getString(2),row))
    //对行为数据按照session粒度进行分组
    var sessionid2ActionsRDD = sessionid2ActionRDD.groupByKey()
    //对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
    //到此为止，获取的数据格式如下：<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
    var userid2PartAggrInfoRDD = sessionid2ActionsRDD.map(x=>mapToPair(x))

    //查询所有用户数据
    var sql = "select * from user_info"
    var userInfoRDD = sqlContext.sql(sql).rdd
    var userid2InfoRDD = userInfoRDD.map(row => (row.getLong(0),row))
    //将session粒度聚合数据，与用户信息进行join
    var userid2FullInfoRDD = userid2PartAggrInfoRDD.join(userid2InfoRDD)
    //对join起来的数据进行拼接，并且返回<sessionid,fullAggrInfo>格式的数据
    var sessionid2FullAggrInfoRDD = userid2FullInfoRDD.map(row=>mapToPair2(row))
    sessionid2FullAggrInfoRDD
  }
  def mapToPair2(tuple:Tuple2[Long,Tuple2[String,Row]]):Tuple2[String,String]={
    var partAggrInfo = tuple._2._1
    var userInfoRow = tuple._2._2
    var sessionid = StringUtils.getFieldFromConcatString(
      partAggrInfo,"\\|",Constants.FIELD_SESSION_ID
    ).getOrElse("")
    var age = userInfoRow.getInt(3)
    var professional = userInfoRow.getString(4)
    var city = userInfoRow.getString(5)
    var sex = userInfoRow.getString(6)
    var fullAggrInfo = partAggrInfo + "|"+ Constants.FIELD_AGE + "=" + age + "|" +
      Constants.FIELD_PROFESSIONAL +
      "=" + professional + "|"+
      Constants.FIELD_CITY + "=" + city + "|" +
      Constants.FIELD_SEX +"=" + sex
    Tuple2[String,String](sessionid,fullAggrInfo)
  }
  /**
    * 过滤session数据
    * @param sessionid2AggrInfoRDD
    * @return
    */
  // 参数的处理
  def filterSession(sessionid2AggrInfoRDD:RDD[(String,String)],
                    taskParam:JSONObject):RDD[(String,String)]={
    //为了使用后面的ValieUtils,所以，首先将所有的筛选参数拼接成一个连接串
    var startAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    var endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    var professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    var cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    var sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    var keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    var categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    var _parameter = f"${getSomeParam(Constants.PARAM_START_AGE,startAge)}${getSomeParam(Constants.PARAM_END_AGE,endAge)}" +
      f"${getSomeParam(Constants.PARAM_PROFESSIONALS,professionals)}${getSomeParam(Constants.PARAM_CITIES,cities)}" +
      f"${getSomeParam(Constants.PARAM_SEX,sex)}${getSomeParam(Constants.PARAM_KEYWORDS,keywords)}" +
      f"${getSomeParam(Constants.PARAM_CATEGORY_IDS,categoryIds)}"
    if(_parameter.endsWith("\\|")){
      _parameter = _parameter.substring(0,_parameter.length-1)
    }
    var parameter = _parameter
    // 根据筛选参数进行过滤
    var applyd = apply(parameter,_:Tuple2[String,String])
    // FIXME:new Function?
    var filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(
      x => applyd(x)
    )
    filteredSessionid2AggrInfoRDD
  }
  def apply(parameter:String,tuple:Tuple2[String,String]): Boolean = {
    var aggrInfo = tuple._2
    //接着，依次按照筛选条件进行过滤
    //按照年龄范围进行过滤（startAge、endAge）
    if(!ValidUtils.between(aggrInfo,Constants.FIELD_AGE,
      parameter,Constants.PARAM_START_AGE,Constants.PARAM_END_AGE)){
      return false
    }
    // 按照职业范围进行过滤
    if(!ValidUtils.in(aggrInfo,Constants.FIELD_PROFESSIONAL,
      parameter,Constants.PARAM_PROFESSIONALS)) return false

    //按照城市范围进行过滤（cities）
    if(!ValidUtils.in(aggrInfo,Constants.FIELD_CITY,parameter,
      Constants.PARAM_CATEGORY_IDS))return false

    //按照性别过滤
    if(!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX,
      parameter, Constants.PARAM_SEX)) return false

    //按照搜索词过滤
    if(!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS,
      parameter, Constants.PARAM_KEYWORDS)) return false

    //按照点击品类id进行搜索,点击品类：应该是点击的商品
    if(!ValidUtils.in(aggrInfo,Constants.FIELD_CLICK_CATEGORY_IDS,
      parameter,Constants.PARAM_CATEGORY_IDS)) return false

    true
  }
  private def getSomeParam(key:String,x:Option[String]): String = x match {
    case Some(y) => f"$key=$y|"
    case None => ""
  }
}

