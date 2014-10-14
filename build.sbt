import sbtassembly.Plugin.{AssemblyKeys, MergeStrategy, PathList}
import AssemblyKeys._

assemblySettings

org.scalastyle.sbt.ScalastylePlugin.Settings

name := "LSHQuery"
 
version := "0.1"
 
scalaVersion := "2.11.2"

test in assembly :={}

scalacOptions ++= Seq("-unchecked", "-deprecation", "-Yno-adapted-args")

libraryDependencies ++= Seq(
  "com.typesafe.akka" % "akka-actor_2.11" % "2.3.6",
  "com.typesafe" % "config" % "1.2.1",
  "org.apache.commons" % "commons-math3" % "3.3"
)
