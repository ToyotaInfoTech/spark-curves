name := "spark-curves"
organization := "io.github.toyotainfotech"

val scalaVersion212 = "2.12.18"
val scalaVersion213 = "2.13.12"
scalaVersion := scalaVersion212
crossScalaVersions := Seq(scalaVersion212, scalaVersion213)
releaseCrossBuild := true

scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Xsource:3")
scalafmtOnCompile := true

val sparkVersion = "3.5.0"
val scalaTestVersion = "3.2.17"

publishMavenStyle := true

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test
)

Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated
Compile / runMain := Defaults.runMainTask(Compile / fullClasspath, Compile / run / runner).evaluated
