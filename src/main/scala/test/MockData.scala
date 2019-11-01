package test
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.{DataFrame,Row,RowFactory,SQLContext}
import org.apache.spark.sql.types.{DataTypes,StructType}
import java.util.ArrayList
import java.util.Random
import scala.Array._
import java.util.UUID
import util.{DateUtils,StringUtils}
import scala.collection.mutable.ArrayBuffer


// 产生实时数据
object MockData {
  def mock(sc:SparkContext,sqlContext:SQLContext):Unit={
    import sqlContext.implicits._
    var rows = new ArrayBuffer[Row]()
    val searchKeywords:Array[String] = Array[String]("火锅", "蛋糕", "重庆辣子鸡", "重庆小面",
                "呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉","怒火八零",
    "秦人面馆","阿迪达斯","耐克","new balance")
    //生成的数据日期为当天日期，设置搜索参数时应注意搜索时间范围
    var date = DateUtils.getTodayDate
    val actions = Array[String]("search", "click", "order", "pay")
    var random = new Random()
    for(i <- range(0,101)) {
      var userid = random.nextInt(100).toString.toLong
      for (j <- 0 until 10) {
        var sessionid = UUID.randomUUID().toString.replace("-","")
        var baseActionTime = f"$date ${random.nextInt(23)}"
        for (k <- 0 until random.nextInt(10)){
          var pageid = random.nextInt(10).toString.toLong
          var actionTime = f"$baseActionTime:${StringUtils.fulfuill(random.nextInt(59).toString)}:" +
                            f"${StringUtils.fulfuill(random.nextInt(59).toString)}"
          var searchKeyword:String = null
          var clickCategoryId:Long = 0
          var clickProductId:Long = 0
          var orderCategoryIds:String = null
          var orderProductIds:String = null
          var payCategoryIds:String = null
          var payProductIds:String = null

          var action = actions(random.nextInt(4))

          action match {
            case "search" => searchKeyword = searchKeywords(random.nextInt(13))
            case "click" => clickCategoryId = random.nextInt(100).toString.toLong
                            clickProductId = random.nextInt(100).toLong
            case "order" => orderCategoryIds =random.nextInt(100).toString
                          orderProductIds = random.nextInt(100).toString
            case "pay" => payCategoryIds = random.nextInt(100).toString
                          payProductIds = random.nextInt(100).toString
            case _ =>
          }
//          println(f"$date $userid $sessionid $pageid $actionTime" +
//            f"$searchKeyword $clickCategoryId $clickProductId $orderCategoryIds" +
//            f"$orderProductIds $payCategoryIds")
          // TODO:隐式转换下
          var row = RowFactory.create(date.asInstanceOf[Object],userid.asInstanceOf[Object],sessionid.asInstanceOf[Object],
                        pageid.asInstanceOf[Object],actionTime.asInstanceOf[Object], searchKeyword.asInstanceOf[Object],
                        clickCategoryId.asInstanceOf[Object],clickProductId.asInstanceOf[Object],
                        orderCategoryIds.asInstanceOf[Object], orderProductIds.asInstanceOf[Object],
                        payCategoryIds.asInstanceOf[Object],payProductIds.asInstanceOf[Object])
          rows += row
        }
      }
    }
    var rowsRDD:RDD[Row] = sc.parallelize(rows.toSeq)
    // 结构化字段->结构化类型
    var schema:StructType = DataTypes.createStructType(Array(
      DataTypes.createStructField("date",DataTypes.StringType,true),
      DataTypes.createStructField("user_id", DataTypes.LongType, true),
      DataTypes.createStructField("session_id", DataTypes.StringType, true),
      DataTypes.createStructField("page_id", DataTypes.LongType, true),
      DataTypes.createStructField("action_time", DataTypes.StringType, true),
      DataTypes.createStructField("search_keyword", DataTypes.StringType, true),
      DataTypes.createStructField("click_category_id", DataTypes.LongType, true),
      DataTypes.createStructField("click_product_id", DataTypes.LongType, true),
      DataTypes.createStructField("order_category_ids", DataTypes.StringType, true),
      DataTypes.createStructField("order_product_ids", DataTypes.StringType, true),
      DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true),
      DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true)
    ))
    var df:DataFrame = sqlContext.createDataFrame(rowsRDD,schema)
    df.registerTempTable("user_visit_action")
//    var sql = f"select * from user_visit_action limit 2"
//    var dt = sqlContext.sql(sql)
    for(_row <- df.take(1)) println()


    rows.clear()
    var sexes:Array[String] = Array("male","female")
    for(i<- 0 until 100){
      var userid = i.toString.toLong
      val username = "user" + i
      val name = "name" + i
      val age = random.nextInt(60)
      val professional = "professional" + random.nextInt(100)
      val city = "city" + random.nextInt(100)
      val sex = sexes(random.nextInt(2))

      var row = RowFactory.create(userid.asInstanceOf[Object],username.asInstanceOf[Object],
                                    name.asInstanceOf[Object],age.asInstanceOf[Object],
                                  professional.asInstanceOf[Object],city.asInstanceOf[Object],sex.asInstanceOf[Object])
      rows += row
    }
    rowsRDD = sc.parallelize(rows.toSeq)

    val schema2 = DataTypes.createStructType(Array(
      DataTypes.createStructField("user_id", DataTypes.LongType, true),
      DataTypes.createStructField("username", DataTypes.StringType, true),
      DataTypes.createStructField("name", DataTypes.StringType, true),
      DataTypes.createStructField("age", DataTypes.IntegerType, true),
      DataTypes.createStructField("professional", DataTypes.StringType, true),
      DataTypes.createStructField("city", DataTypes.StringType, true),
      DataTypes.createStructField("sex", DataTypes.StringType, true)))
    val df2 = sqlContext.createDataFrame(rowsRDD, schema2)
//    for (_row <- df2.take(1)) {println(_row)}
    df2.registerTempTable("user_info")
  }
}
