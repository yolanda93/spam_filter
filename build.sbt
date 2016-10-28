// build.sbt file

name := "spam_filter"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.4.1",
  "org.apache.spark" %% "spark-network-common" % "1.4.1" ,
  "org.apache.spark" %% "spark-network-shuffle" % "1.4.1",
  "org.apache.spark" %% "spark-unsafe" % "1.4.1"
)