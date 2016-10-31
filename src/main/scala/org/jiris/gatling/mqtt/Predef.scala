package org.jiris.gatling.mqtt

import org.jiris.gatling.mqtt.protocol.MqttProtocol
import org.jiris.gatling.mqtt.request.builder.MqttRequestBuilder
import io.gatling.core.session.Expression
import io.gatling.core.config.GatlingConfiguration

object Predef {
 def mqtt(implicit configuration:GatlingConfiguration) = MqttProtocol.MqttProtocolKey.defaultValue(configuration)
  def mqtt(requestName: Expression[String]) =
    new MqttRequestBuilder(requestName)
}
