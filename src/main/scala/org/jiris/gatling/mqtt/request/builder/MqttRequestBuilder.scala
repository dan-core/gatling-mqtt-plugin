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
  retain: Boolean,
  username: Option[Expression[String]],
  password: Option[Expression[String]],
  clientId: Option[Expression[String]])

case class MqttRequestBuilder(requestName: Expression[String]) {
  def publish(
    topic: Expression[String],
    payload: Expression[String],
    qos: QoS,
    retain: Boolean,
    userName: Option[Expression[String]] = None,
    password: Option[Expression[String]] = None,
    clientId: Option[Expression[String]] = None): MqttRequestActionBuilder =
    new MqttRequestActionBuilder(MqttAttributes(
      requestName,
      topic,
      payload,
      qos,
      retain,
      userName,
      password,
      clientId))
}
