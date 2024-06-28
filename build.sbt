ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.13"

val sparkVersion = "3.5.0"


v2



// Spark
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql"  % sparkVersion
libraryDependencies +="com.github.pureconfig" %% "pureconfig"% "0.16.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.17" % Test
libraryDependencies +="com.typesafe" % "config" % "1.2.1"


lazy val root = (project in file("."))
  .settings(
    name := "bank-spark-scala-project"
  )
