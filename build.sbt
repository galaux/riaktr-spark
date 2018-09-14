import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      scalaVersion := "2.11.8",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "Simple Project",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "2.3.1",

      scalaTest % Test,
      "com.holdenkarau" % "spark-testing-base_2.11" % "2.3.1_0.10.0" % Test,
      "org.apache.spark" %% "spark-hive" % "2.3.1" % Test
    ),
    dependencyOverrides ++= Seq(
      "com.google.code.findbugs" % "jsr305" % "3.0.2",
      "com.google.guava" % "guava" % "11.0.2",
      "commons-net" % "commons-net" % "2.2",
      "io.netty" % "netty" % "3.9.9.Final"
    ),
    run / javaOptions ++= Seq(
      "-Xms512M",
      "-Xmx2048M",
      "-XX:MaxPermSize=2048M",
      "-XX:+CMSClassUnloadingEnabled"),
    Test / fork := true,
    Test / parallelExecution := false
  )

