import sbt.Keys._
import sbt._
import sbtassembly.AssemblyPlugin.autoImport.{MergeStrategy, assemblyJarName, assemblyMergeStrategy}
import sbtassembly.PathList
import sbtassembly.AssemblyKeys._

object Settings {
  lazy val VERSION = "0.3.0"
  lazy val SCALA_VERSION = "2.11.12"
  lazy val ORGANIZATION = "com.span"

  lazy val commonSettings = Defaults.itSettings ++ Seq(
    organization := ORGANIZATION,
    conflictManager in Compile := ConflictManager.strict,
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint"),
    initialize := {
      val _ = initialize.value
      if (sys.props("java.specification.version") != "1.8")
        sys.error("Java 8 is required for this project.")
    },
    parallelExecution in Test := false,
    scalaVersion := SCALA_VERSION,
    version := VERSION
  )

  lazy val assemblySettings = Seq(
    assemblyJarName in assembly := name.value + ".jar",
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case _ => MergeStrategy.last
    }
  )

  lazy val resolvers = Seq(
    Resolver.jcenterRepo,
    "Central" at "http://repo1.maven.org/maven2/",
    "apache-snapshots" at "http://repository.apache.org/snapshots/"
  )

  lazy val commonDependencies: Seq[ModuleID] = Seq(
    "org.apache.spark" %% "spark-core" % "2.3.1",
    "org.apache.spark" %% "spark-sql" % "2.3.1",
    "org.scalatest" %% "scalatest" % "3.0.4" % "test",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
  )

  lazy val batchDependencies: Seq[ModuleID] = Seq(
    "com.ibm.stocator" % "stocator" % "1.0.24",
    "com.typesafe" % "config" % "1.3.2"
  )

  lazy val demoDependencies: Seq[ModuleID] = Seq(

  )
}
