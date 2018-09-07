name := "Simple Project"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "com.holdenkarau" % "spark-testing-base_2.11" % "2.3.1_0.10.0" % "test"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.1" % "test"

fork in Test := true
parallelExecution in Test := false
javaOptions ++= Seq(
  "-Xms512M",
  "-Xmx2048M",
  "-XX:MaxPermSize=2048M",
  "-XX:+CMSClassUnloadingEnabled")

