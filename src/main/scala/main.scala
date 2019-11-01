import org.apache.spark.{SparkConf,SparkContext}
object main{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
              .setAppName("net work Count").setMaster("local")
            //    StreamingExamples.setStreamingLogLevels()
                val sc = new SparkContext(conf)
              val rdd = sc.parallelize(List(1,2,3,1,5,10)).map(_*3)
                val maprdd = rdd.filter(_>10).collect()
        //        println(rdd.reduce(_+_))
                for(arg <- maprdd) {
                  print(arg + " ")
                }
  }
}