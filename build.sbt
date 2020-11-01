name := "aggregator"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "3.0.1"

val spark = Seq(
  "org.apache.spark" %% "spark-core",
  "org.apache.spark" %% "spark-sql"
).map (_ % sparkVersion )

assemblyJarName in assembly := "aggregator.jar"
mainClass in assembly := Some("com.example.spark.Application")

libraryDependencies ++= Seq("org.scalatest" %% "scalatest" % "3.2.2" % Test) ++ spark