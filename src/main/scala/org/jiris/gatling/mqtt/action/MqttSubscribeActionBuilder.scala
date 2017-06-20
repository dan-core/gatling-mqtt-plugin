package org.jiris.gatling.mqtt.action

import org.jiris.gatling.mqtt.protocol.MqttProtocol
import org.jiris.gatling.mqtt.protocol.MqttComponents
import org.jiris.gatling.mqtt.request.builder.MqttAttributes
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.protocol.ProtocolComponentsRegistry
import org.fusesource.mqtt.client.MQTT
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.action.Action
import io.gatling.core.protocol.ProtocolComponents
import com.typesafe.scalalogging.Logger
import io.gatling.core.session.Session

class MqttSubscribeActionBuilder(mqttAttributes: MqttAttributes) extends ActionBuilder {
  def mqttComponents(protocolComponentsRegistry: ProtocolComponentsRegistry): MqttComponents =
      protocolComponentsRegistry.components(MqttProtocol.MqttProtocolKey)
      
  def build(ctx:ScenarioContext, next: Action): Action = {
    val mqttProtocol = mqttComponents(ctx.protocolComponentsRegistry).mqttProtocol

  new PahoMqttSubscribeAction(
            ctx.coreComponents.statsEngine,
            mqttAttributes, 
            mqttProtocol, 
            next)
  }
}