version := "0.1"
scalaVersion := "2.12.18"
name := "spark_konsultant_test_task"
organization := "com.test.task"

val sparkVersion = "3.5.5"
val scalatestVersion = "3.2.19"
val specs2Version = "4.21.0"
val pureconfigVersion = "0.17.8"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core",
  "org.apache.spark" %% "spark-sql"
).map(_ % sparkVersion)

val otherDependencies = Seq(
  "com.github.pureconfig" %% "pureconfig" % pureconfigVersion,
)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % scalatestVersion,
  "org.specs2" %% "specs2-core" % specs2Version
).map(_ % Test)

libraryDependencies ++= sparkDependencies ++ testDependencies ++ otherDependencies

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

assembly / mainClass := Some("com.test.task.Main")
