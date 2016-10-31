package org.jiris.gatling.mqtt.protocol

import io.gatling.core.protocol.ProtocolComponents

case class MqttComponents(mqttProtocol: MqttProtocol) extends ProtocolComponents {
  def onExit: Option[io.gatling.core.session.Session ⇒ Unit] = None
  def onStart: Option[io.gatling.core.session.Session ⇒ io.gatling.core.session.Session] = None
}