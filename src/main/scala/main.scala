import org.apache.spark.{SparkConf, SparkContext}
object main{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
              .setAppName("net work Count").setMaster("local")
      .set("spark.sql.broadcastTimeout","40000")
            //    StreamingExamples.setStreamingLogLevels()
                val sc = new SparkContext(conf)
              val rdd = sc.parallelize(Seq(List(1,2,3),List(4,5,6))).flatMap(x=>x).map(_*3)
                val maprdd = rdd.filter(_>10).collect()
        //        println(rdd.reduce(_+_))
                for(arg <- maprdd) {
                  print(arg + " ")
                }
//    var v = getS(6)
//    var t = v match {
//      case Some(x) => x
//      case None => ""
//    }
//    println(t)f

  }
  def getS(x:Int):Option[String]={
    if (x > 0){
      Some("have")
    }else{
      None
    }

  }
}