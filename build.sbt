name := "index"

version := "0.1"

scalaVersion := "2.13.1"

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)

libraryDependencies ++= Seq(
  "org.scalactic" %% "scalactic" % "3.1.0",
  "org.scalatest" %% "scalatest" % "3.1.0" % "test",
  "com.github.ben-manes.caffeine" % "caffeine" % "2.7.0",
  "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.1",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.7.2",
  "com.google.guava" % "guava" % "27.1-jre",
  "org.apache.commons" % "commons-lang3" % "3.8.1"
)