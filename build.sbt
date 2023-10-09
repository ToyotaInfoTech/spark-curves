name := "spark-curves"
description := "Apache Spark extension library to calculate curve similarity, clustering, etc."
licenses := List("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))
homepage := Some(url("https://github.com/ToyotaInfoTech/spark-curves"))

organization := "io.github.toyotainfotech"
organizationName := "ToyotaInfoTech"
organizationHomepage := Some(url("https://github.com/ToyotaInfoTech"))

val scalaVersion212 = "2.12.18"
val scalaVersion213 = "2.13.12"
scalaVersion := scalaVersion212
crossScalaVersions := Seq(scalaVersion212, scalaVersion213)

scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Xsource:3")
scalafmtOnCompile := true

val sparkVersion = "3.5.0"
val scalaTestVersion = "3.2.17"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test
)

Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated
Compile / runMain := Defaults.runMainTask(Compile / fullClasspath, Compile / run / runner).evaluated

import ReleaseTransformations.*
releaseCrossBuild := true
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  releaseStepCommandAndRemaining("+publishSigned"),
  releaseStepCommand("sonatypeBundleRelease"),
  setNextVersion,
  commitNextVersion,
  pushChanges
)

pomIncludeRepository := { _ => false }
publishMavenStyle := true
publishTo := sonatypePublishToBundle.value
sonatypeCredentialHost := "s01.oss.sonatype.org"
versionScheme := Some("semver-spec")
scmInfo := Some(
  ScmInfo(
    url("https://github.com/ToyotaInfoTech/spark-curves"),
    "scm:git@github.com:ToyotaInfoTech/spark-curves.git"
  )
)
developers := List(
  Developer(
    id = "piyo7",
    name = "Takatomo Torigoe",
    email = "piyo7@users.noreply.github.com",
    url = url("https://github.com/piyo7")
  )
)
