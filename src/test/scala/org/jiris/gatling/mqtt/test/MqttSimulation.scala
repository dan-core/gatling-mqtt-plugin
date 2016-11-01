package org.jiris.gatling.mqtt.test

import io.gatling.core.Predef._
import org.fusesource.mqtt.client.QoS
import scala.concurrent.duration._

import org.jiris.gatling.mqtt.Predef._
import org.jiris.gatling.mqtt.protocol.MqttProtocol

class MqttSimulation extends Simulation {
  val mqttConf= mqtt.host("ssl://localhost:8883")
  val scn = scenario("MQTT Test")
    .exec(mqtt("request")
    .publish(topic = "foo", payload = "Hello", qos = QoS.AT_LEAST_ONCE, retain = false))
  setUp(
    scn
      .inject(atOnceUsers(1)))
    .protocols(mqttConf)
}
