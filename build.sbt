lazy val root = (project in file("."))
.settings(
    organization := "com.hipages",
    version := "0.0.1",
    scalaVersion := "2.11.11",
    name := "HiPages Data Engineering Test",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.4.0",
      "org.apache.spark" %% "spark-sql" % "2.4.0",
      "org.scalactic" %% "scalactic" % "3.0.5",
      "org.scalatest" %% "scalatest" % "3.0.5" % "test"
      )
    )

resolvers += Resolver.mavenLocal
fork in run := true
