import sbtassembly.Plugin.{AssemblyKeys, MergeStrategy, PathList}
import AssemblyKeys._

assemblySettings

org.scalastyle.sbt.ScalastylePlugin.Settings

name := "LSHQuery"
 
version := "0.1"
 
scalaVersion := "2.11.2"

test in assembly :={}

scalacOptions ++= Seq("-unchecked", "-deprecation", "-Yno-adapted-args")


