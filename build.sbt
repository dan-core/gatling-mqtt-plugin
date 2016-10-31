name := "gatling-mqtt"

version := "0.0.4-SNAPSHOT"

scalaVersion := "2.11.5"

enablePlugins(GatlingPlugin)

libraryDependencies ++= Seq(
  "io.gatling" % "gatling-core" % "2.2.2" % "provided",

  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "io.gatling.highcharts" % "gatling-charts-highcharts" % "2.2.2",
  "io.gatling"            % "gatling-test-framework"    % "2.2.2",

  "org.fusesource.mqtt-client" % "mqtt-client" % "1.10"
)

// Gatling contains scala-library
assemblyOption in assembly := (assemblyOption in assembly).value
  .copy(includeScala = false)
