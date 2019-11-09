package spark

import java.util.Date
import scala.collection.mutable.{ArrayBuffer, HashMap}
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import conf.ConfigurationManager
import constant.Constants
import test.MockData
import util._
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject
import dao.factory.DAOFactory
import domain._
import groovy.lang.Tuple
import jdbc.JDBCHelper
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.Tuple2
import scala.collection.mutable
import scala.util.Random
import scala.util.control.Breaks

object UserVisitSessionAnalyzeSpark {
    Logger.getLogger("org").setLevel(Level.ERROR)

    //  REW 这个参数是web平台传过来的
    def main(args: Array[String]): Unit = {
        System.setProperty("hadoop.home.dir", "D:\\hiicy\\Download\\Spark大型电商项目实战软件包\\jdk\\hadoop-2.5.0")

        var conf = new SparkConf()
            .setAppName(Constants.SPARK_APP_NAME)
            .setMaster("local")
            .set("spark.executor.memory", "300M")
            //      .set("spark.eventLog.enabled","true")
            .set("spark.sql.broadcastTimeout", "40000")
            .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
            .registerKryoClasses(Array(classOf[CategorySortKey])) // 用Kryo序列化 优化

        var sc: SparkContext = new SparkContext(conf)
        var sqlContext = getSQLContext(sc)

        //     生成模拟测试数据
        mockData(sc, sqlContext)
        // 创建需要使用的DAO组件
        val taskDAO = DAOFactory.getTaskDAO
        //那么就首先得查询出来指定的任务，并获取任务的查询参数
        var taskid = Option[Long](2) // ParamUtils.getTaskIdFromArgs(args)
        taskid match {
            case None => System.exit(0)
            case Some(x) => x
        }

        var task = taskDAO.findById(taskid.getOrElse(0))
        //find的时候就把任务参数设置了
        var taskParam = JSON.parseObject(task.taskParam) //转成一个json对象
        //    println(taskParam)
        //如果要进行session粒度的数据聚合，
        //首先要从user_visit_action表中，查询出来指定日期范围内的数据
        var actionRDD = getActionRDDByDateRange(sqlContext, taskParam)

        var sessionid2actionRDD = actionRDD.map(row => (row.getString(2), row))
                                            .persist(StorageLevel.MEMORY_ONLY_SER) // 内存序列化 持久化

        //    //聚合
        //    //首先，可以将行为数据按照session_id进行groupByKey分组
        //    //此时的数据粒度就是session粒度了，然后可以将session粒度的数据与用户信息数据进行join
        //    //然后就可以获取到session粒度的数据，同时数据里面还包含了session对应的user信息
        ////    到这里为止，获取的数据是<sessionid,(sessionid,searchKeywords,
        ////    clickCategoryIds,age,professional,city,sex)>
        var sessionid2AggrInfoRDD = aggregateBySession(sqlContext, sessionid2actionRDD)

        //接着，就要针对session粒度的聚合数据，按照使用者指定的筛选参数进行数据过滤
        //相当于我们自己编写的算子，是要访问外面的任务参数对象的
        //匿名内部类（算子函数），访问外部对象，是要给外部对象使用final 我用的val 修饰的

        //重构，同时进行过滤和统计
        var sessionAggrStatAccumulator: Accumulator[String] =
            sc.accumulator("")(new SessionAggrStatAccumulator())
        //生成公共RDD：通过筛选条件的session的访问明细数据 :RDD[(String,Row)]
        var filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(
            sessionid2AggrInfoRDD, taskParam, sessionAggrStatAccumulator)
            .persist(StorageLevel.MEMORY_ONLY_SER)

        var sessionid2detailRDD = getSessionid2detailRDD(
            filteredSessionid2AggrInfoRDD, sessionid2actionRDD
        ).persist(StorageLevel.MEMORY_ONLY_SER)

        //计算出各个范围的session占比，并写入MySQL
        randomExtractSession(task.getTaskid, filteredSessionid2AggrInfoRDD, sessionid2actionRDD)

        /**
          * session聚合统计（统计出访问时长和访问步长，各个区间的session数量占总session数量的比例）
          */
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value, task.getTaskid)

        // 获取top10热门品类
        var top10CategoryList = getTop10Category(task.getTaskid, sessionid2detailRDD)

        //获取top10活跃session
        getTop10Session(sc, task.getTaskid, top10CategoryList, sessionid2detailRDD)
        sessionid2detailRDD.count()
        sc.stop()
    }

    /**
      * 获取SQLContext
      * 如果在本地测试环境的话，那么生成SQLContext对象
      * 如果在生产环境运行的话，那么就生成HiveContext对象
      *
      * @param sc SparkContext
      * @return SQLContext
      */
    def getSQLContext(sc: SparkContext): SQLContext = {
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
        if (local) {
            new SQLContext(sc)
        }
        else {
            new HiveContext(sc)
        }
    }

    /**
      * 生成模拟数据
      * 只有是本地模式，才会生成模拟数据
      *
      * @param sc
      * @param sqlContext
      */
    def mockData(sc: SparkContext, sqlContext: SQLContext): Unit = {
        var local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
        if (local) {
            MockData.mock(sc, sqlContext)
        }
    }

    /**
      * 获取指定日期范围内的用户访问行为数据
      *
      * @param sqlContext SQLContext
      * @param taskParam  任务参数
      * @return 行为数据RDD
      */
    def getActionRDDByDateRange(sqlContext: SQLContext, taskParam: JSONObject): RDD[Row] = {
        import sqlContext.implicits._
        //先在Constants.java中添加任务相关的常量
        //String PARAM_START_DATE = "startDate"
        //String PARAM_END_DATE = "endDate"

        var startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE) match {
            case Some(x) => x
            case None => ""
        }
        var endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE) match {
            case Some(x) => x
            case None => ""
        }

        // 模拟了一张user_visit_action表
        var sql = f"select * from user_visit_action where date>='$startDate'" +
            f" and date<='$endDate'"
        //    var sql = f"select date from user_visit_action limit 2"
        //    println(sql)
        var actionDF: DataFrame = sqlContext.sql(sql)
        //    af.show()
        actionDF.rdd
    }

    def mapToPair(tuple: (String, Iterable[Row])): Tuple2[Long, String] = {
        var sessionid = tuple._1
        var iterator = tuple._2.iterator
        var searchKeywordsBuffer = new StringBuffer("")
        var clickCategoryIdsBuffer = new StringBuffer("")
        var userid: Long = 0L

        //session的起始、结束时间
        var startTime: Date = null
        var endTime: Date = null
        //session的访问步长
        var stepLength: Int = 0

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

            //计算session 开始 结束时间
            var actionTime = DateUtils.parseTime(row.getString(4)).orNull

            if (startTime == null) {
                startTime = actionTime
            }
            if (endTime == null) {
                endTime = actionTime
            }

            if (actionTime.before(startTime)) startTime = actionTime
            if (actionTime.after(endTime)) endTime = actionTime

            // 计算session访问步长
            stepLength += 1
        }

        var searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString)
        var clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString)

        //计算session访问时长（秒）
        var visitLength = (endTime.getTime - startTime.getTime) / 1000

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
        //String FIELD_VISIT_LENGTH = "visitLength"
        //String FIELD_STEP_LENGTH = "stepLength"
        var partAggrInfo = f"${Constants.FIELD_SESSION_ID}=$sessionid|" +
            f"${Constants.FIELD_SEARCH_KEYWORDS}=$searchKeywords|" +
            f"${Constants.FIELD_CLICK_CATEGORY_IDS}=$clickCategoryIds|" +
            f"${Constants.FIELD_VISIT_LENGTH}=$visitLength|" +
            f"${Constants.FIELD_STEP_LENGTH}=$stepLength|" +
            f"${Constants.FIELD_START_TIME}=${DateUtils.formatTime(startTime)}"
        new Tuple2[Long, String](userid, partAggrInfo)
    }

    /**
      * 对行为数据按session粒度进行聚合
      *
      * @param actionRDD 行为数据RDD
      * @return session粒度聚合数据
      */
    def aggregateBySession(sqlContext: SQLContext,
                           sessionid2actionRDD: RDD[(String,Row)]):
                                            RDD[Tuple2[String, String]] = {

        //对行为数据按照session粒度进行分组
        var sessionid2ActionsRDD = sessionid2actionRDD.groupByKey()
        //对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
        //到此为止，获取的数据格式如下：<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
        var userid2PartAggrInfoRDD = sessionid2ActionsRDD.map(x => mapToPair(x))

        //查询所有用户数据
        var sql = "select * from user_info"
        var userInfoRDD = sqlContext.sql(sql).rdd
        var userid2InfoRDD = userInfoRDD.map(row => (row.getLong(0), row))
        //将session粒度聚合数据，与用户信息进行join
        var userid2FullInfoRDD = userid2PartAggrInfoRDD.join(userid2InfoRDD)
        //对join起来的数据进行拼接，并且返回<sessionid,fullAggrInfo>格式的数据
        var sessionid2FullAggrInfoRDD = userid2FullInfoRDD.map(row => mapToPair2(row))
        sessionid2FullAggrInfoRDD
    }

    def mapToPair2(tuple: Tuple2[Long, Tuple2[String, Row]]): Tuple2[String, String] = {
        var partAggrInfo = tuple._2._1
        //    println(partAggrInfo)
        var userInfoRow = tuple._2._2
        var sessionid = StringUtils.getFieldFromConcatString(
            partAggrInfo, "\\|", Constants.FIELD_SESSION_ID
        ).getOrElse("")
        val age = userInfoRow.getInt(3)
        var professional = userInfoRow.getString(4)
        var city = userInfoRow.getString(5)
        var sex = userInfoRow.getString(6)

        var fullAggrInfo = partAggrInfo + "|" + Constants.FIELD_AGE + "=" + age + "|" +
            Constants.FIELD_PROFESSIONAL +
            "=" + professional + "|" +
            Constants.FIELD_CITY + "=" + city + "|" +
            Constants.FIELD_SEX + "=" + sex
        Tuple2[String, String](sessionid, fullAggrInfo)
    }

    /**
      * 过滤session数据，并进行聚合统计
      *
      * @param sessionid2AggrInfoRDD
      * @param taskParam
      * @return
      */
    // TODO:参数的处理,
    def filterSessionAndAggrStat(sessionid2AggrInfoRDD: RDD[(String, String)],
                                 taskParam: JSONObject, sessionAggrStatAccumulator: Accumulator[String]): RDD[(String, String)] = {
        //    println(f"过滤之前session还有的值:${sessionid2AggrInfoRDD.count()}")
        //为了使用后面的ValieUtils,所以，首先将所有的筛选参数拼接成一个连接串
        var startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
        var endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
        var professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
        var cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
        var sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
        var keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
        var categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

        var _parameter = f"${getSomeParam(Constants.PARAM_START_AGE, startAge)}${getSomeParam(Constants.PARAM_END_AGE, endAge)}" +
            f"${getSomeParam(Constants.PARAM_PROFESSIONALS, professionals)}${getSomeParam(Constants.PARAM_CITIES, cities)}" +
            f"${getSomeParam(Constants.PARAM_SEX, sex)}${getSomeParam(Constants.PARAM_KEYWORDS, keywords)}" +
            f"${getSomeParam(Constants.PARAM_CATEGORY_IDS, categoryIds)}"

        if (_parameter.endsWith("\\|")) {
            _parameter = _parameter.substring(0, _parameter.length - 1)
        }
        val parameter = _parameter

        // 根据筛选参数进行过滤
        var applyd = apply(sessionAggrStatAccumulator, parameter, _: Tuple2[String, String]) //偏函数 scala写法
        // FIXME:new Function? 加个序列化idprivate val serialVersionUID: Long = 1L
        var filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(
            x => applyd(x)
        )
        filteredSessionid2AggrInfoRDD
    }

    def apply(sessionAggrAccumulator: Accumulator[String], parameter: String, tuple: Tuple2[String, String]): Boolean = {
        var aggrInfo = tuple._2
        //接着，依次按照筛选条件进行过滤
        //按照年龄范围进行过滤（startAge、endAge）
        if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE,
            parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
            return false
        }
        // 按照职业范围进行过滤
        if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL,
            parameter, Constants.PARAM_PROFESSIONALS)) return false

        //按照城市范围进行过滤（cities）
        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter,
            Constants.PARAM_CATEGORY_IDS)) return false

        //按照性别过滤
        if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX,
            parameter, Constants.PARAM_SEX)) return false

        //按照搜索词过滤
        if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS,
            parameter, Constants.PARAM_KEYWORDS)) return false

        //按照点击品类id进行搜索,点击品类：应该是点击的商品
        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS,
            parameter, Constants.PARAM_CATEGORY_IDS)) return false

        //如果经过了之前的多个过滤条件之后，程序能够走到这里
        //那么说明该session是通过了用户指定的筛选条件的，也就是需要保留的session
        //那么就要对session的访问时长和访问步长进行统计，
        //根据session对应的范围进行相应的累加计数
        //只要走到这一步，那么就是需要计数的session
        sessionAggrAccumulator.add(Constants.SESSION_COUNT)

        //计算出session的访问时长和访问步长的范围，并进行相应的累加
        var visitLength = StringUtils.getFieldFromConcatString(aggrInfo,
            "\\|", Constants.FIELD_VISIT_LENGTH).getOrElse("0").toLong
        var stepLength = StringUtils.getFieldFromConcatString(aggrInfo,
            "\\|", Constants.FIELD_STEP_LENGTH).getOrElse("0").toLong
        calculateVisitLength(visitLength)
        calculatedStepLength(stepLength)

        /**
          * 计算访问时长范围*/
        def calculateVisitLength(visitLength: Long): Unit = visitLength match {
            case _x if _x >= 1 && _x <= 3 => sessionAggrAccumulator.add(Constants.TIME_PERIOD_1s_3s)
            case _x if _x >= 4 && _x <= 6 => sessionAggrAccumulator.add(Constants.TIME_PERIOD_4s_6s)
            case _x if _x >= 7 && _x <= 9 => sessionAggrAccumulator.add(Constants.TIME_PERIOD_7s_9s)
            case _x if _x >= 10 && _x <= 30 => sessionAggrAccumulator.add(Constants.TIME_PERIOD_10s_30s)
            case _x if _x > 30 && _x <= 60 => sessionAggrAccumulator.add(Constants.TIME_PERIOD_30s_60s)
            case _x if _x > 60 && _x <= 180 => sessionAggrAccumulator.add(Constants.TIME_PERIOD_1m_3m)
            case _x if _x > 180 && _x <= 600 => sessionAggrAccumulator.add(Constants.TIME_PERIOD_3m_10m)
            case _x if _x > 600 && _x <= 1800 => sessionAggrAccumulator.add(Constants.TIME_PERIOD_10m_30m)
            case _x if _x > 1800 => sessionAggrAccumulator.add(Constants.TIME_PERIOD_30m)
            case _ =>
        }

        /*计算访问步长*/
        def calculatedStepLength(stepLength: Long): Unit = stepLength match {
            case _x if _x >= 1 && _x <= 3 => sessionAggrAccumulator.add(Constants.STEP_PERIOD_1_3)
            case _x if _x >= 4 && _x <= 6 => sessionAggrAccumulator.add(Constants.STEP_PERIOD_4_6)
            case _x if _x >= 7 && _x <= 9 => sessionAggrAccumulator.add(Constants.STEP_PERIOD_7_9)
            case _x if _x >= 10 && _x <= 30 => sessionAggrAccumulator.add(Constants.STEP_PERIOD_10_30)
            case _x if _x > 30 && _x <= 60 => sessionAggrAccumulator.add(Constants.STEP_PERIOD_30_60)
            case _x if _x > 60 => sessionAggrAccumulator.add(Constants.STEP_PERIOD_60)
            case _ =>
        }

        true
    }

    private def getSomeParam(key: String, x: Option[String]): String = x match {
        case Some(y) => f"$key=$y|"
        case None => ""
    }

    def randomExtract(tuple: Tuple2[String, String]): Tuple2[String, String] = {
        var aggrInfo = tuple._2
        var startTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|",
            Constants.FIELD_START_TIME).getOrElse("")
        var dateHour = DateUtils.getDateHour(startTime)
        new Tuple2[String, String](dateHour, aggrInfo)
    }

    /**
      * 随机抽取session
      */
    def randomExtractSession(sc:SparkContext,taskid: Long, sessionid2AggrInfoRDD: RDD[(String, String)],
                             sessionid2actionRDD: RDD[(String, Row)]): Unit = {
        var time2sessionidRDD = sessionid2AggrInfoRDD.map(
            // 转成<datehour,aggrInfo>
            randomExtract
        )

        // 每个小时各有多少session
        var countMap = time2sessionidRDD.countByKey() // 根据key来统计 key相同的各有多少

        //将<yyyy-mm-dd_hh,count>格式的map转换成<yyyy-mm-dd,<hh,count>>的格式，方便后面使用
        // REW:用可变hash表重构完成！
        var dateHourCountMap: mutable.HashMap[String, mutable.HashMap[String, Long]] =
        mutable.HashMap[String, mutable.HashMap[String, Long]]()
        countMap.foreach { countEntry =>
            var dateHour = countEntry._1
            var date = dateHour.split("_")(0)
            var hour = dateHour.split("_")(1)

            var count = countEntry._2.toLong
            if (!dateHourCountMap.contains(date)) {
                var hourCount: mutable.HashMap[String, Long] = mutable.HashMap[String, Long]()
                dateHourCountMap.put(date, hourCount)
            }
            dateHourCountMap(date).put(hour, count)
        }

        //开始实现按照时间比例随机抽取算法
        //总共抽取100个session，先按照天数平分 100可以 变量 化
        var extractNumberPerDay = 100 / dateHourCountMap.size
        //每一天每一个小时抽取session的索引，<date,<hour,(3,5,20,200)>>
        var dateHourExtractMap: mutable.HashMap[String, mutable.HashMap[String,
            mutable.ListBuffer[Int]]] = mutable.HashMap[String, mutable.HashMap[String,
            mutable.ListBuffer[Int]]]()

        var random = new Random()

        dateHourCountMap.foreach { dateHourCountEntry =>
            var date = dateHourCountEntry._1
            var hourCountMap: mutable.HashMap[String, Long] = dateHourCountEntry._2
            // 计算出这一天的session总数
            var sessionCount = 0L
            for (hourCount <- hourCountMap.values) {
                sessionCount += hourCount
            }

            var hourExtractMap = dateHourExtractMap.getOrElse(date, null)
            if (hourExtractMap == null) {
                hourExtractMap = mutable.HashMap[String, mutable.ListBuffer[Int]]()
                dateHourExtractMap.put(date, hourExtractMap)
            }
            // 该天每个时间的统计
            hourCountMap.foreach { hourCountEntry =>
                var hour = hourCountEntry._1
                var count = hourCountEntry._2

                //计算每个小时的session数量，占据当天总session数量的比例，直接乘以每天要抽取的数量
                //就可以算出当前小时所需抽取的session数量
                var hourExtractNumber = ((count.toDouble / sessionCount.toDouble)
                    * extractNumberPerDay).toInt
                if (hourExtractNumber > count) hourExtractNumber = count.toInt

                //先获取当前小时的存放随机数list
                var extractIndexList: mutable.ListBuffer[Int] = hourExtractMap.getOrElse(hour, null)
                if (extractIndexList == null) {
                    extractIndexList = mutable.ListBuffer[Int]()
                    hourExtractMap.put(hour, extractIndexList)
                }
                //生成上面计算出来的数量的随机数
                for (i <- 0 until hourExtractNumber) {
                    var extractIndex = random.nextInt(count.toInt)
                    // 不重复
                    while (extractIndexList.contains(extractIndex)) {
                        extractIndex = random.nextInt(count.toInt)
                    }
                    extractIndexList.append(extractIndex)
                }
            }
        }



        var dateHourExtractMapBroadcast: Broadcast[mutable.HashMap[String,
                                                    mutable.HashMap[String,
                                                    mutable.ListBuffer[Int]]]]
                                        = sc.broadcast(dateHourExtractMap)

        /**
          * 第三步：遍历每天每小时的session，根据随机索引抽取
          */
        //执行groupByKey算子，得到<dateHour,(session aggrInfo)>
        var time2sessionsRDD = time2sessionidRDD.groupByKey()
        //    println(f"还剩: ${time2sessionsRDD.count()}")
        //我们用flatMap算子遍历所有的<dateHour,("session aggrInfo")>格式的数据
        //然后会遍历每天每小时的session
        //如果发现某个session恰巧在我们指定的这天这小时的随机抽取索引上
        //那么抽取该session，直接写入MySQL的random_extract_session表
        //将抽取出来的session id返回回来，形成一个新的JavaRDD<String>
        //然后最后一步，用抽取出来的sessionid去join它们的访问行为明细数据写入session表

        var randtime2sessions = randomTime(taskid, dateHourExtractMapBroadcast, _: Tuple2[String, Iterable[String]])
        // FIXMEup:匿名函数的task serialable 没有弄好
        // (Fixmeup:貌似匿名函数里有其他类就要报错
        println(f"time2sessionsRDD:${time2sessionsRDD.count()}")
        var extractSessionidsRDD = time2sessionsRDD.flatMap {
            randtime2sessions
        }
        println(f"apply")

        /**
          * 第四步：获取抽取出来的session的明细数据
          */
        var extractSessionDetailRDD = extractSessionidsRDD.join(sessionid2actionRDD)
//        var call = write2Mysql(taskid, _: Tuple2[String, Tuple2[String, Row]])
//        println(f"extractsesfisng:${extractSessionDetailRDD.count()}")
//        extractSessionDetailRDD.foreach(call) // fixmeup:写到mysql 要挂起？
    }

    def randomTime(taskid: Long,
                   dateHourExtractMapBroadcast: Broadcast[mutable.HashMap[String, mutable.HashMap[String,
                                                        mutable.ListBuffer[Int]]]] ,
                   tuple: Tuple2[String, Iterable[String]]): Iterable[Tuple2[String, String]] = {
        var extractSessionids = new mutable.ArrayBuffer[Tuple2[String, String]]()
        var dateHour = tuple._1
        var date = dateHour.split("_")(0)
        var hour = dateHour.split("_")(1)
        var iterator = tuple._2.iterator
        var dateHourExtractMap = dateHourExtractMapBroadcast.value

        //拿到这一天这一小时的随机索引
        var extractIndexList = dateHourExtractMap(date)(hour)
        var sessionRandomExtractDAO = DAOFactory.getSessionRandomExtractDAO

        var index = 0
        while (iterator.hasNext) {
            var sessionAggrInfo = iterator.next()
            if (extractIndexList.contains(index)) {
                var sessionid = StringUtils.getFieldFromConcatString(sessionAggrInfo,
                    "\\|", Constants.FIELD_SESSION_ID).getOrElse("")
                //写入mysql
                var sessionRandomExtract = new SessionRandomExtract()
                sessionRandomExtract.setTaskid(taskid)
                sessionRandomExtract.setSessionid(sessionid)
                sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(
                    sessionAggrInfo, "\\|", Constants.FIELD_START_TIME
                ).getOrElse(""))
                sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(
                    sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS
                ).getOrElse(""))
                sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(
                    sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS
                ).getOrElse(""))
                //插入mysql
                sessionRandomExtractDAO.insert(sessionRandomExtract)
                extractSessionids.append(new Tuple2[String, String](sessionid, sessionid))
                //            println("inset")
            }
            index += 1
        }
        extractSessionids.toSeq
    }

    def write2Mysql(taskid: Long, tuple: Tuple2[String, Tuple2[String, Row]]): Unit = {
        var row = tuple._2._2
        var sessionDetail = new SessionDetail()
        sessionDetail.setTaskid(taskid)
        sessionDetail.setUserid(row.getLong(1))
        sessionDetail.setSessionid(row.getString(2))
        sessionDetail.setPageid(row.getLong(3))
        sessionDetail.setActionTime(row.getString(4))
        sessionDetail.setSearchKeyword(row.getString(5))
        sessionDetail.setClickCategoryId(row.getLong(6))
        sessionDetail.setClickProductId(row.getLong(7))
        sessionDetail.setOrderCategoryIds(row.getString(8))
        sessionDetail.setOrderProductIds(row.getString(9))
        sessionDetail.setPayCategoryIds(row.getString(10))
        sessionDetail.setPayProductIds(row.getString(11))
        // FIXME:我觉得这里可以把DAO抽离出来
        var sessionDetailDAO = DAOFactory.getSessionDetailDAO
        // FIXMEUP:
        sessionDetailDAO.insert(sessionDetail)
    }

    /*计算各session的访问时长和访问步长所占比例
       * 计算各session范围占比，并写入MySQL
       */
    def calculateAndPersistAggrStat(value: String, taskid: Long): Unit = {
        //    println(f"none $value")
        // 从Accumulator统计串中获取值
        var session_count = StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.SESSION_COUNT).getOrElse("0").toDouble
        var visit_length_1s_3s = StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.TIME_PERIOD_1s_3s).getOrElse("0").toDouble
        var visit_length_4s_6s = StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.TIME_PERIOD_4s_6s).getOrElse("0").toDouble
        var visit_length_7s_9s = StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.TIME_PERIOD_7s_9s).getOrElse("0").toDouble
        var visit_length_10s_30s = StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.TIME_PERIOD_10s_30s).getOrElse("0").toDouble
        var visit_length_30s_60s = StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.TIME_PERIOD_30s_60s).getOrElse("0").toDouble
        var visit_length_1m_3m = StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.TIME_PERIOD_1m_3m).getOrElse("0").toDouble
        var visit_length_3m_10m = StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.TIME_PERIOD_3m_10m).getOrElse("0").toDouble
        var visit_length_10m_30m = StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.TIME_PERIOD_10m_30m).getOrElse("0").toDouble
        var visit_length_30m = StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.TIME_PERIOD_30m).getOrElse("0").toDouble

        var step_length_1_3 = StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.STEP_PERIOD_1_3).getOrElse("0").toDouble
        var step_length_4_6 = StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.STEP_PERIOD_4_6).getOrElse("0").toDouble
        var step_length_7_9 = StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.STEP_PERIOD_7_9).getOrElse("0").toDouble
        var step_length_10_30 = StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.STEP_PERIOD_10_30).getOrElse("0").toDouble
        var step_length_30_60 = StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.STEP_PERIOD_30_60).getOrElse("0").toDouble
        var step_length_60 = StringUtils.getFieldFromConcatString(
            value, "\\|", Constants.STEP_PERIOD_60).getOrElse("0").toDouble

        var visit_length_1s_3s_ratio: Double = 0.0
        var visit_length_4s_6s_ratio: Double = 0.0
        var visit_length_7s_9s_ratio: Double = 0.0
        var visit_length_10s_30s_ratio: Double = 0.0
        var visit_length_30s_60s_ratio: Double = 0.0
        var visit_length_1m_3m_ratio: Double = 0.0
        var visit_length_3m_10m_ratio: Double = 0.0
        var visit_length_10m_30m_ratio: Double = 0.0
        var visit_length_30m_ratio: Double = 0.0

        var step_length_1_3_ratio: Double = 0.0
        var step_length_4_6_ratio: Double = 0.0
        var step_length_7_9_ratio: Double = 0.0
        var step_length_10_30_ratio: Double = 0.0
        var step_length_30_60_ratio: Double = 0.0
        var step_length_60_ratio: Double = 0.0
        println(session_count)
        session_count match {
            //计算各个访问时长和访问步长的范围
            case x if x.toInt > 0 =>
                println("---")
                //计算各个访问时长和访问步长的范围
                visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                    visit_length_1s_3s / session_count, 2)
                visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                    visit_length_4s_6s / session_count, 2)
                visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                    visit_length_7s_9s / session_count, 2)
                visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                    visit_length_10s_30s / session_count, 2)
                visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                    visit_length_30s_60s / session_count, 2)
                visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                    visit_length_1m_3m / session_count, 2)
                visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                    visit_length_3m_10m / session_count, 2)
                visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                    visit_length_10m_30m / session_count, 2)
                visit_length_30m_ratio = NumberUtils.formatDouble(
                    visit_length_30m / session_count, 2)

                step_length_1_3_ratio = NumberUtils.formatDouble(
                    step_length_1_3 / session_count, 2)
                step_length_4_6_ratio = NumberUtils.formatDouble(
                    step_length_4_6 / session_count, 2)
                step_length_7_9_ratio = NumberUtils.formatDouble(
                    step_length_7_9 / session_count, 2)
                step_length_10_30_ratio = NumberUtils.formatDouble(
                    step_length_10_30 / session_count, 2)
                step_length_30_60_ratio = NumberUtils.formatDouble(
                    step_length_30_60 / session_count, 2)
                step_length_60_ratio = NumberUtils.formatDouble(
                    step_length_60 / session_count, 2)

            case _ =>
        }


        var sessionAggrStat = new SessionAggrStat()
        sessionAggrStat.setTaskid(taskid)
        sessionAggrStat.setSession_count(session_count.toInt)
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio)
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio)
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio)
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio)
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio)
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio)
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio)
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio)
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio)

        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio)
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio)
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio)
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio)
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio)
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio)

        var sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO
        sessionAggrStatDAO.insert(sessionAggrStat)
    }

    /**
      * 获取通过筛选条件的session的访问明细数据RDD
      *
      * @param sessionid2aggrInfoRDD
      * @param sessionid2actionRDD
      * @return
      */
    def getSessionid2detailRDD(sessionid2aggrInfoRDD: RDD[(String, String)], sessionid2actionRDD: RDD[(String, Row)]):
    RDD[Tuple2[String, Row]] = {
        var sessionid2detailRDD = sessionid2aggrInfoRDD.join(sessionid2actionRDD).map { row =>
            new Tuple2[String, Row](row._1, row._2._2)
        }
        sessionid2detailRDD
    }

    /**
      * 获取Top10热门品类
      *
      */
    def getTop10Category(taskid: Long,
                         sessionid2detailRDD: RDD[(String, Row)]):Array[(CategorySortKey, String)] = {
        /**
          * 第一步：获取符合条件的session访问过的所有品类
          * 已经获取了
          */

        //获取session访问过的所有品类id
        //访问过指的是点击、下单、支付的品类
        var categoryidRDD = sessionid2detailRDD.flatMap { tuple =>
            var row = tuple._2
            var list = new mutable.ArrayBuffer[(Long, Long)]()
            var clickCategoryId = row.get(6)
            if (clickCategoryId != null) list.append(new Tuple2[Long, Long](
                                    clickCategoryId.asInstanceOf[Long], clickCategoryId.asInstanceOf[Long]))
            var orderCategoryIds = row.getString(8)
            if (orderCategoryIds != null) {
                var orderCategoryIdsSplited = orderCategoryIds.split(",")
                for (orderCategoryId <- orderCategoryIdsSplited) {
                    list.append(new Tuple2[Long, Long](orderCategoryId.toLong, orderCategoryId.toLong))
                }
            }

            var payCategoryIds = row.getString(10)
            if (payCategoryIds != null) {
                val payCategoryIdsSplited = payCategoryIds.split(",")
                for (payCategoryId <- payCategoryIdsSplited) {
                    list.append(new Tuple2[Long, Long](payCategoryId.toLong, payCategoryId.toLong))
                }
            }
            list
        }

        //必须去重
        //如果不去重的话，会出现重复的categoryid，排序会对重复的categoryid以及countInfo进行排序
        //最后很可能会拿到重复的数据 这里应该是筛选出操作过的categoryid 后面再对操作计数
        categoryidRDD = categoryidRDD.distinct()

        /**
          * 第二步：计算各品类的点击、下单和支付的次数
          */
        //访问明细中，其中三种访问行为是点击、下单和支付
        //分别来计算各品类点击、下单和支付的次数，可以先对访问明细数据进行过滤
        //分别过滤点击、下单和支付行为，然后通过map、reduceByKey等算子进行计算

        //计算各个品类点击次数
        def getClickCategoryId2CountRDD(sessionid2detailRDD: RDD[(String, Row)]): RDD[(Long, Long)] = {
            var clickActionRDD = sessionid2detailRDD.filter { tuple =>
                var row = tuple._2
                var flag = false
                if (row.get(6) != null) flag = true
                flag
            }
            var clickCategoryIdRDD = clickActionRDD.map { tuple =>
                var clickCategoryId = tuple._2.getLong(6)
                new Tuple2[Long, Long](clickCategoryId, 1L)
            }

            var clickCategoryId2CountRDD = clickCategoryIdRDD.reduceByKey { (v1, v2) =>
                v1 + v2
            }
            clickCategoryId2CountRDD
        }

        var clickCategoryId2CountRDD = //categoryid:点击量
            getClickCategoryId2CountRDD(sessionid2detailRDD)

        //计算各个品类的下单次数
        def getOrderCategoryId2CountRDD(sessionid2detailRDD: RDD[(String, Row)]): RDD[(Long, Long)] = {
            var orderActionRDD = sessionid2detailRDD.filter { tuple =>
                var row = tuple._2
                var flag = false
                if (row.get(8) != null) flag = true
                flag
            }
            var orderCategoryIdRDD = orderActionRDD.flatMap { tuple => // 这个用法很好
                var orderCategoryIds = tuple._2.getString(8)
                var orderCategoryIdsSplited = orderCategoryIds.split(",")
                var list = new ArrayBuffer[(Long, Long)]()
                for (orderCategoryId <- orderCategoryIdsSplited) {
                    list.append(new Tuple2[Long, Long](orderCategoryId.toLong, 1L))
                }
                list
            }

            var orderCategoryId2CountRDD = orderCategoryIdRDD.reduceByKey { (v1, v2) =>
                v1 + v2
            }
            orderCategoryId2CountRDD
        }

        val orderCategoryId2CountRDD = getOrderCategoryId2CountRDD(sessionid2detailRDD)

        //计算各个品类的支付次数
        def getPayCategoryId2CountRDD(sessionid2detailRDD: RDD[(String, Row)]): RDD[(Long, Long)] = {
            var payActionRDD = sessionid2detailRDD.filter { tuple =>
                var row = tuple._2
                var flag = false
                if (row.get(10) != null) flag = true
                flag
            }
            var payCategoryIdRDD = payActionRDD.flatMap { tuple => // 这个用法很好
                var payCategoryIds = tuple._2.getString(10)
                var payCategoryIdsSplited = payCategoryIds.split(",")
                var list = new ArrayBuffer[(Long, Long)]()
                for (payCategoryId <- payCategoryIdsSplited) {
                    list.append(new Tuple2[Long, Long](payCategoryId.toLong, 1L))
                }
                list
            }

            var payCategoryId2CountRDD = payCategoryIdRDD.reduceByKey { (v1, v2) =>
                v1 + v2
            }
            payCategoryId2CountRDD
        }

        val payCategoryId2CountRDD = getPayCategoryId2CountRDD(sessionid2detailRDD)

        /**
          * 第三步：join各品类与它的点击、下单和支付的次数
          * caetoryidRDD中包含了所有符合条件的session访问过的品类id
          *
          * 上面分别计算出来的三份各品类的点击、下单和支付的次数，可能不是包含所有品类的，
          * 比如，有的品类就只是被点击过，但是没有人下单和支付
          * 所以，这里就不能使用join操作，而是要使用leftOutJoin操作，
          * 就是说，如果categoryidRDD不能join到自己的某个数据，比如点击、下单或支付数据，
          * 那么该categoryidRDD还是要保留下来的
          * 只不过，没有join到的那个数据就是0了
          */
        var categoryid2countRDD = joinCategoryAndData(categoryidRDD, clickCategoryId2CountRDD,
            orderCategoryId2CountRDD, payCategoryId2CountRDD)
        // 第四步 自定义二次排序key

        /**
          * 第五步：将数据映射成<SortKey,info>格式的RDD，然后进行二次排序（降序）
          */
        var sortkey2countRDD = categoryid2countRDD.map { tuple =>
            var countInfo = tuple._2
            var clickCount = StringUtils.getFieldFromConcatString(countInfo,
                            "\\|",Constants.FIELD_CLICK_COUNT).getOrElse("0").toLong
            var orderCount = StringUtils.getFieldFromConcatString(countInfo,
                "\\|",Constants.FIELD_ORDER_COUNT).getOrElse("0").toLong
            var payCount = StringUtils.getFieldFromConcatString(countInfo,
                "\\|",Constants.FIELD_PAY_COUNT).getOrElse("0").toLong
            var sortkey = new CategorySortKey(clickCount,orderCount,payCount)
            new Tuple2[CategorySortKey,String](sortkey,countInfo)
        }
        var sortedCategoryCountRDD = sortkey2countRDD.sortByKey(ascending = false)
        /**
          * 第六步：用kake(10)取出top10热门品类，并写入MySQL
          */
        var top10CategoryDAO = DAOFactory.getTop10CategoryDAO
        var top10CategoryList = sortedCategoryCountRDD.take(10)
        for(tuple:(CategorySortKey,String) <- top10CategoryList){
            var countInfo = tuple._2
            var categoryid = StringUtils.getFieldFromConcatString(countInfo,
                "\\|",Constants.FIELD_CATEGORY_ID).getOrElse("-1").toLong
            var clickCount = StringUtils.getFieldFromConcatString(countInfo,
                "\\|",Constants.FIELD_CLICK_COUNT).getOrElse("0").toLong
            var orderCount = StringUtils.getFieldFromConcatString(countInfo,
                "\\|",Constants.FIELD_ORDER_COUNT).getOrElse("0").toLong
            var payCount = StringUtils.getFieldFromConcatString(countInfo,
                "\\|",Constants.FIELD_PAY_COUNT).getOrElse("0").toLong
            var category = new Top10Category
            category.taskid_=(taskid)
            category.categoryid_=(categoryid)
            category.clickCount_=(clickCount)
            category.orderCount_=(orderCount)
            category.payCount_=(payCount)
            top10CategoryDAO.insert(category)
        }
        println("top10category insert done")
        top10CategoryList
    }
    //join各品类与它的点击、下单和支付的次数
    def joinCategoryAndData(categoryidRDD: RDD[(Long, Long)],
                            clickCategoryId2CountRDD: RDD[(Long, Long)],
                            orderCategoryId2CountRDD: RDD[(Long, Long)],
                            payCategoryId2CountRDD: RDD[(Long, Long)]
                           ): RDD[(Long, String)] = {
        //如果使用leftOuterJoin就有可能出现右边那个RDD中join过来时没有值
        //所以Tuple2中的第二个值用Optional<Long>类型就代表可能有值也可能没有值
        var tmpJoinRDD: RDD[(Long, (Long, Option[Long]))] = categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD)
        var tmpMapRDD = tmpJoinRDD.map { tuple =>
            var categoryid = tuple._1
            var optional = tuple._2._2
            var clickCount = optional match {
                case Some(x) => x
                case None => 0L
            }
            var value:String = s"${Constants.FIELD_CATEGORY_ID}=$categoryid|" +
                s"${Constants.FIELD_CLICK_COUNT}=$clickCount"
            new Tuple2[Long, String](categoryid, value)
        }
        tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryId2CountRDD).map { tuple =>
            var categoryid = tuple._1
            var value = tuple._2._1
            var orderCount = tuple._2._2 match {
                case Some(x) => x
                case None => 0L
            }
            value = s"$value|${Constants.FIELD_ORDER_COUNT}=$orderCount"
            new Tuple2[Long, String](categoryid, value)
        }
        tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryId2CountRDD).map { tuple =>
            var categoryid = tuple._1
            var value = tuple._2._1
            var payCount = tuple._2._2 match {
                case Some(x) => x
                case None => 0L
            }
            value = s"$value|${Constants.FIELD_PAY_COUNT}=$payCount"
            new Tuple2[Long, String](categoryid, value)
        }
        tmpMapRDD
    }

    /**
      * 获取top10活跃session
      * @param sc
      * @param taskid
      * @param top10CategoryList
      * @param sessionid2detailRDD
      */
    def getTop10Session(sc: SparkContext, taskid: Long,
                        top10CategoryList: Array[(CategorySortKey, String)],
                        sessionid2detailRDD: RDD[(String, Row)]):Unit={
        /**
          * 第一步：将top10热门品类的id生成一份RDD
          */
        println("get top 10 session")
        var top10CategoryIdList = new ArrayBuffer[(Long,Long)]()
        for (category <- top10CategoryList) {
            val categoryid = StringUtils.getFieldFromConcatString(category._2, "\\|",
                    Constants.FIELD_CATEGORY_ID).getOrElse("-1").toLong
            top10CategoryIdList.append(new Tuple2[Long, Long](categoryid, categoryid))
        }
        val top10CategoryIdRDD = sc.parallelize(top10CategoryIdList)
        /**
          * 第二步：计算top10品类被各session点击的次数
          */
        var sessionid2detailsRDD = sessionid2detailRDD.groupByKey()

        var categoryid2sessionCountRDD = sessionid2detailsRDD.flatMap{ tuple=> //flatmap 的序列展平 有点东西
            var sessionid = tuple._1
            var iterator = tuple._2.iterator
            var categoryCountMap = new mutable.HashMap[Long,Long]()
            //计算出该session对每个品类的点击次数
            while (iterator.hasNext){
                var row = iterator.next()
                if (row.get(6)!=null){
                    var categoryClickid = row.getLong(6)
                    var count = categoryCountMap.get(categoryClickid)
                    count match {
                        case Some(x)=>categoryCountMap.update(categoryClickid,x+1)
                        case None => categoryCountMap.put(categoryClickid,1L)
                    }
                }
            }
            //{categoryid:count}
//            categoryCountMap
            // 返回结果为<categoryid,session:count>格式
            var list = new ArrayBuffer[(Long,String)]()
            for(categoryCountEntry <- categoryCountMap){
                var categoryid = categoryCountEntry._1
                var count = categoryCountEntry._2
                var value = f"$sessionid,$count"
                list.append(new Tuple2[Long,String](categoryid,value))
            }
            list
        }
        //获取到top10热门品类被各个session点击的次数
        var top10CategorySessionCountRDD = top10CategoryIdRDD.join(categoryid2sessionCountRDD) //有就是有，没有就是没有
            .map(tuple => (tuple._1,tuple._2._2))
        /**
          * 第三步：分组取topN算法实现，获取每个品类的top10活跃用户
          */
        var  top10CategorySessionCountsRDD = top10CategorySessionCountRDD.groupByKey()
        var top10SessionRDD = top10CategorySessionCountsRDD.flatMap{ tuple=>
            var categoryid = tuple._1
            var iterator = tuple._2.iterator
            println("top10category every",tuple._2.size)
            var top10Sessions = new Array[String](10)
            while (iterator.hasNext){
                var sessionCount =iterator.next()
                var count = sessionCount.split(",")(1).toLong
                var loop = new Breaks
                // REW:TopN算法
                loop.breakable{
                for(i <- top10Sessions.indices){
                    if (top10Sessions(i) == null) {
                        top10Sessions(i) = sessionCount
                        loop.break() //todo: break is not supported
                    }else {
                        var _count = top10Sessions(i).split(",")(1).toLong
                        if(count>_count){
                            //从排序数组最后一位开始，到i位所有数据往后挪一位
                            for(j <- Range(9,i,-1)){
                                //往后移，其实就是后面的占有了前面的值
                                top10Sessions(j) = top10Sessions(j-1)
                            }
                            top10Sessions(i)=sessionCount
                            loop.break()
                        }
                        //如果sessionCount比i位的sessionCount要小，继续外层for循环
                    }
                }
                }
            }

            //将数据写入MySQL表//将数据写入MySQL表
            var list = new ArrayBuffer[(String,String)]()
            val top10SessionDAO = DAOFactory.getTop10SessionDAO
            println("top10Session",top10Sessions.mkString(","))
            for (sessionCount <- top10Sessions) {
                if (sessionCount != null) {
                    val sessionid = sessionCount.split(",")(0)
                    val count = sessionCount.split(",")(1).toLong
                    //将top10session插入MySQL表
                    val top10Session = new Top10Session()
                    top10Session.taskid_=(taskid)
                    top10Session.categoryid_=(categoryid)
                    top10Session.sessionid_=(sessionid)
                    top10Session.clickCount_=(count)
                    top10SessionDAO.insert(top10Session)
                    list.append(new Tuple2[String, String](sessionid, sessionid))
                }
            }
            list
        }
        println("top10SessionRDD count",top10SessionRDD.count())
//        将toptop10活跃session的明细数据写入MySQL
        val sessionDetailRDD = top10SessionRDD.join(sessionid2detailRDD)
        sessionDetailRDD.foreach { tuple=>
            var row = tuple._2._2
            var sessionDetail = new SessionDetail()
            sessionDetail.setTaskid(taskid)
            sessionDetail.setUserid(row.getLong(1))
            sessionDetail.setSessionid(row.getString(2))
            sessionDetail.setPageid(row.getLong(3))
            sessionDetail.setActionTime(row.getString(4))
            sessionDetail.setSearchKeyword(row.getString(5))
            sessionDetail.setClickCategoryId(row.getLong(6))
            sessionDetail.setClickProductId(row.getLong(7))
            sessionDetail.setOrderCategoryIds(row.getString(8))
            sessionDetail.setOrderProductIds(row.getString(9))
            sessionDetail.setPayCategoryIds(row.getString(10))
            sessionDetail.setPayProductIds(row.getString(11))
            var sessionDetailDAO = DAOFactory.getSessionDetailDAO
            sessionDetailDAO.insert(sessionDetail)
        }
    }
}

