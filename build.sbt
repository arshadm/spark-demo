
lazy val sparkVersion = "2.0.2"

lazy val commonSettings = Seq(
  organization := "io.spinor",
  version := "1.0.0-SNAPSHOT",
  scalaVersion := "2.11.8"
)

lazy val sparkDemo = (project in file(".")).
  enablePlugins(JavaAppPackaging).
  settings(commonSettings: _*).
  settings(
    name := "spark-demo",
    libraryDependencies ++= Seq(
      "com.typesafe" % "config" % "1.3.1",
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-mllib" % sparkVersion,
      "org.apache.spark" %% "spark-graphx" % sparkVersion,
      "com.google.guava" % "guava" % "20.0",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
      "ch.qos.logback" % "logback-classic" % "1.1.3",
      "org.scalatest" %% "scalatest" % "2.2.6" % "test"
    )
  )

fork in run := true
