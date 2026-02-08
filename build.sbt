ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.17"

lazy val root = (project in file("."))
  .settings(
    name := "tessera",
    libraryDependencies ++= Seq("at.yawk.lz4" % "lz4-java" % "1.10.3")
  )
