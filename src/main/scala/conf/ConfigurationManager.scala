package conf
/*
* 配置管理组件，就在静态代码块中，编写读取配置文件的代码，这样的话，
* 第一次外界代码调用这个ConfigurationManager类的静态方法的时候，就会加载配置文件中的数据，
* 而且，放在静态代码块中，还有一个好处，就是类的初始化在整个JVM生命周期内，有且仅有一次，
 * 配置文件只会加载一次，然后以后就是重复使用，效率比较高
*/
import java.io.InputStream
import java.util.Properties

object ConfigurationManager {
  var prop = new Properties()
  try{
    var in = ConfigurationManager.
                            getClass.
                            getClassLoader.
                            getResourceAsStream("my.properties")
    prop.load(in)
  } catch {
    case e:Throwable=>e.printStackTrace()
  }
  def getProperty(key:String):String={
    prop.getProperty(key)
  }
  def getInteger(key:String):Int={
    var value = getProperty(key)
    try{
      return value.toInt
    }catch {
      case e:NumberFormatException => e.printStackTrace()
    }
    0
  }
  def getBoolean(key:String):Boolean={
    var value = getProperty(key)
    try{
      return value.toBoolean
    }catch {
      case e:Throwable => e.printStackTrace()
    }
    false
  }
}
