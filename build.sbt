import sbtassembly.Plugin.{AssemblyKeys, MergeStrategy, PathList}
import AssemblyKeys._

assemblySettings

org.scalastyle.sbt.ScalastylePlugin.Settings

name := "LSHQuery"
 
version := "0.1"
 
scalaVersion := "2.10.4"

test in assembly :={}

scalacOptions ++= Seq("-unchecked", "-deprecation", "-Yno-adapted-args")

libraryDependencies ++= Seq(
  "com.typesafe.akka" % "akka-actor_2.10" % "2.3.6",
  "com.typesafe.akka" % "akka-contrib_2.10" % "2.3.6",
  "com.typesafe" % "config" % "1.2.1",
  "org.apache.commons" % "commons-math3" % "3.3",
  "org.apache.spark" % "spark-mllib_2.10" % "1.1.0",
  "org.scalatest" % "scalatest_2.10" % "2.2.2"
)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case PathList("META-INF", xs @ _*) =>
    (xs map {_.toLowerCase}) match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) => MergeStrategy.discard
      case _ => MergeStrategy.discard
    }
  case _ => MergeStrategy.first
}
}