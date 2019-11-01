package test

import conf.ConfigurationManager

object ConfigurationManagerTest {
  def main(args: Array[String]): Unit = {
    var testkey1 = ConfigurationManager.getProperty("testkey1")
    var testkey2 = ConfigurationManager.getProperty("testkey2")
    println(testkey1)
    println(testkey2)
  }
}
