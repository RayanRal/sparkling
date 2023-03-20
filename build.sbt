ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "sparkling",
    idePackagePrefix := Some("com.rayanral")
  )

libraryDependencies += "org.apache.spark" % "spark-core_2.12" % "3.3.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.12" % "3.3.0"
libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % "3.3.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.3.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.15" % Test
libraryDependencies += "io.github.embeddedkafka" %% "embedded-kafka" % "3.4.0" % Test