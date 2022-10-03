ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.16"

lazy val root = (project in file("."))
  .settings(
    name := "lc-csv-to-dataframe"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.2",
  "org.apache.spark" %% "spark-sql" % "3.2.2",
  "org.scalactic" %% "scalactic" % "3.2.12" % Test,
  "org.scalatest" %% "scalatest" % "3.2.12" % Test,
  "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.2",
  "com.typesafe" % "config" % "1.4.2",
  "org.reflections" % "reflections" % "0.10.2",
  "com.amazonaws" % "aws-java-sdk" % "1.12.300",
  "com.amazonaws" % "aws-java-sdk-sqs" % "1.12.312",
  "org.json4s" %% "json4s-jackson" % "3.5.1"
)