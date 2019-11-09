name := "sparkE-business"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.38"
// https://mvnrepository.com/artifact/com.alibaba/fastjson
libraryDependencies += "com.alibaba" % "fastjson" % "1.2.62"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1" exclude("org.jboss.netty", "netty")
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.5.1" % "runtime"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.5.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.5.1"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.5.1"
libraryDependencies += "org.projectlombok" % "lombok" % "1.18.10"
libraryDependencies += "it.unimi.dsi" % "fastutil" % "8.3.0"






