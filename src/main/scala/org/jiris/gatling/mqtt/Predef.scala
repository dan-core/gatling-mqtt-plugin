package org.jiris.gatling.mqtt

import org.jiris.gatling.mqtt.protocol.MqttProtocol
import org.jiris.gatling.mqtt.request.builder.MqttRequestBuilder
import io.gatling.core.session.Expression

object Predef {
 def mqtt = MqttProtocol

  def mqtt(requestName: Expression[String]) =
    new MqttRequestBuilder(requestName)

}
