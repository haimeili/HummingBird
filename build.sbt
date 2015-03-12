import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin.{MergeStrategy, PathList}

assemblySettings

name := "LSHQuery"
 
version := "0.1"
 
scalaVersion := "2.10.4"

test in assembly :={}

scalacOptions ++= Seq("-unchecked", "-deprecation", "-Yno-adapted-args", "-feature")

fork := true

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

libraryDependencies ++= Seq(
  "com.typesafe.akka" % "akka-contrib_2.10" % "2.3.8",
  "com.typesafe" % "config" % "1.2.1",
  "org.scalatest" % "scalatest_2.10" % "2.2.4",
  "org.scalanlp" % "breeze-math_2.10" % "0.4",
  "com.typesafe.akka" % "akka-testkit_2.10" % "2.3.8"
)


mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
  case x if Assembly.isConfigFile(x) =>
    MergeStrategy.concat
  case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
    MergeStrategy.rename
  case PathList("META-INF", xs @ _*) =>
    (xs map {_.toLowerCase}) match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
        MergeStrategy.discard
      case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "plexus" :: xs =>
        MergeStrategy.discard
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
        MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.first
    }
  case _ => MergeStrategy.first
}
}