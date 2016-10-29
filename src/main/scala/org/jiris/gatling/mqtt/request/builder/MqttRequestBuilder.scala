package org.jiris.gatling.mqtt.request.builder

import org.jiris.gatling.mqtt.action.MqttRequestActionBuilder
import org.jiris.gatling.mqtt.protocol.MqttProtocol
import io.gatling.core.session.Expression
import org.fusesource.mqtt.client.QoS

case class MqttAttributes(
  requestName: Expression[String],
  topic: Expression[String],
  payload: Expression[String],
  qos: QoS,
  retain: Boolean)

case class MqttRequestBuilder(requestName: Expression[String]) {
  def publish(
    topic: Expression[String],
    payload: Expression[String],
    qos: QoS,
    retain: Boolean): MqttRequestActionBuilder =
    new MqttRequestActionBuilder(MqttAttributes(
      requestName,
      topic,
      payload,
      qos,
      retain))
}
