ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "day28-spark",
    version := "1.0",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-streaming" % "3.5.3",
      "org.apache.spark" %% "spark-sql" % "3.5.3"
    )
  )
