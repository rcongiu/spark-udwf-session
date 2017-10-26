import sbt.Resolver

name := "CustomUDWF"

version := "0.1"

scalaVersion := "2.11.11"
val sparkVersion = "2.2.0"

libraryDependencies ++= Seq (
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",

  // test dependencies
  "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.7.2" % "test",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)


resolvers ++= Seq(
  "SnowPlow Repo" at "http://maven.snplow.com/releases/",
  "Maven Repository" at "https://mvnrepository.com",
  Resolver.sonatypeRepo("public"), "Confluent Maven Repo" at "http://packages.confluent.io/maven/"
)

parallelExecution in Test := false
