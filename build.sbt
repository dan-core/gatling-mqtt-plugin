name := "gatling-mqtt"

version := "0.0.7-SNAPSHOT"

scalaVersion := "2.11.5"

enablePlugins(GatlingPlugin)

libraryDependencies ++= Seq(
  "io.gatling" % "gatling-core" % "2.2.2" % "provided",

  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "io.gatling.highcharts" % "gatling-charts-highcharts" % "2.2.2",
  "io.gatling"            % "gatling-test-framework"    % "2.2.2",

  "org.fusesource.mqtt-client" % "mqtt-client" % "1.10",
  "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % "1.1.0"
)

assemblyMergeStrategy in assembly := {
  case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

// Gatling contains scala-library
assemblyOption in assembly := (assemblyOption in assembly).value
  .copy(includeScala = false)
