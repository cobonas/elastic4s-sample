name := "elastic4s"

version := "0.1"

scalaVersion := "2.12.8"

val elastic4sVersion = "6.5.1"
libraryDependencies ++= Seq(
  "com.sksamuel.elastic4s" %% "elastic4s-core" % elastic4sVersion,
  // for the http client
  "com.sksamuel.elastic4s" %% "elastic4s-http" % elastic4sVersion,
  // if you want to use reactive streams
  "com.sksamuel.elastic4s" %% "elastic4s-http-streams" % elastic4sVersion,
  // testing
  "com.sksamuel.elastic4s" %% "elastic4s-testkit" % elastic4sVersion % "test",
  "com.sksamuel.elastic4s" %% "elastic4s-embedded" % elastic4sVersion % "test"
)