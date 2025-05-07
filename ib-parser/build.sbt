import org.typelevel.sbt.tpolecat.*

ThisBuild / organization := "com.parser"
ThisBuild / scalaVersion := "3.4.0"

ThisBuild / tpolecatDefaultOptionsMode := VerboseMode

resolvers += Resolver.mavenLocal

lazy val root = (project in file(".")).settings(
  name := "ib-parser",
  libraryDependencies ++= Seq(
    "com.interactivebrokers" % "tws-api" % "10.36.02",
    "org.tpolecat" %% "skunk-core" % "0.6.4",
    "org.typelevel" %% "cats-effect" % "3.5.3",
    // concurrency abstractions and primitives (Concurrent, Sync, Async etc.)
    "org.typelevel" %% "cats-effect-kernel" % "3.5.3",
    // standard "effect" library (Queues, Console, Random etc.)
    "org.typelevel" %% "cats-effect-std" % "3.5.3",
    "org.typelevel" %% "munit-cats-effect" % "2.0.0" % Test)
)
