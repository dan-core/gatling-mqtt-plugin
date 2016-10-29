package org.jiris.gatling.mqtt.test

import io.gatling.core.Predef._
import org.fusesource.mqtt.client.QoS
import scala.concurrent.duration._

import org.jiris.gatling.mqtt.Predef._
import org.jiris.gatling.mqtt.protocol.MqttProtocol
import io.gatling.core.config.GatlingConfiguration

class MqttSimulation extends Simulation {
  val mqttConf= mqtt.MqttProtocolKey.defaultValue(configuration).host("tcp://dcdevbox110:1883")
  val scn = scenario("MQTT Test")
    .exec(mqtt("request")
    .publish("foo", "Hello", QoS.AT_LEAST_ONCE, retain = false))
   System.out.println()
  setUp(
    scn
      .inject(constantUsersPerSec(1) during(1 seconds)))
    .protocols(mqttConf)
}
