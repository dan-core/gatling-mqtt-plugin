package org.jiris.gatling.mqtt.action

import akka.actor.ActorRef
import org.jiris.gatling.mqtt.protocol.MqttProtocol
import org.jiris.gatling.mqtt.request.builder.MqttAttributes
import io.gatling.core.Predef._
import io.gatling.core.action.{Action, ExitableAction}
import io.gatling.commons.stats.{KO,OK}
import io.gatling.core.stats.StatsEngine
import io.gatling.core.session._
import io.gatling.commons.util.TimeHelper._
import io.gatling.commons.validation.Validation
import io.gatling.core.stats.message.ResponseTimings
import scala.util.control.NonFatal
import java.util.UUID
import org.eclipse.paho.client.mqttv3._
import org.fusesource.mqtt.client.CallbackConnection
import org.fusesource.mqtt.client.QoS

class PahoMqttRequestAction(
  override val statsEngine: StatsEngine,
  override val mqttAttributes: MqttAttributes,
  override val mqttProtocol: MqttProtocol,
  override val next: Action)
    extends PahoMqttBaseAction(statsEngine, mqttAttributes, mqttProtocol, next)  {

  override def connectAndExecute(session: Session, connection: ConnectionBuilder) {
    connection.options.setConnectionTimeout(0)
    connection.client.setCallback(new MqttCallback {
      def connectionLost(error: Throwable) {
        println(s"CONN: Connection lost for ${connection.get.client.getClientId}")
        error.printStackTrace
      }
  
      def messageArrived(topic: String, message: MqttMessage) {}
  
      def deliveryComplete(token: IMqttDeliveryToken) {}
    })
    
    connection.client.connect(connection.options, null, new IMqttActionListener {
      override def onSuccess(token: IMqttToken) {
        println("CONN: Connected " + connection.get.client.getClientId)
        println("CONN: username: " + connection.get.client.getClientId + ", password: "+ mqttAttributes.password)
        doAction(session, token.getClient)
      }
      
      override def onFailure(token: IMqttToken, value: Throwable) {
        println("CONN: Connect failed " + connection.get.client.getClientId)
        value.printStackTrace()
        mqttAttributes.requestName(session).map { resolvedRequestName =>
          statsEngine.logResponse(
            session, 
            resolvedRequestName, 
            ResponseTimings(nowMillis,nowMillis),
            KO,
            None, 
            Some(value.getMessage)
          )
        }
        next ! session
        token.getClient.disconnect
      }
    })
  }
  
  override def doAction(session: Session, connection: IMqttAsyncClient) {
    mqttAttributes.requestName(session).flatMap { resolvedRequestName =>
      mqttAttributes.topic(session).flatMap { resolvedTopic =>
        sendRequest(
          resolvedRequestName,
          connection,
          resolvedTopic,
          mqttAttributes.payload,
          mqttAttributes.qos match {
            case QoS.AT_MOST_ONCE => 0
            case QoS.AT_LEAST_ONCE => 1
            case QoS.EXACTLY_ONCE => 2
          },
          mqttAttributes.retain,
          session)
      }
    }
  }
  
  private def sendRequest(
      requestName: String,
      connection: IMqttAsyncClient,
      topic: String,
      payload: Expression[String],
      qos: Int,
      retain: Boolean,
      session: Session): Validation[Unit] = {
    payload(session).map { resolvedPayload =>
      val requestStartDate = nowMillis
      if(!connection.isConnected()) {
        println("CONN: Disconnected!! " + connection.getClientId)
      }
      try {
      connection.publish(
        topic, 
        resolvedPayload.getBytes, 
        qos, 
        retain,
        null,
        new IMqttActionListener {
          override def onSuccess(token: IMqttToken) = writeData(isSuccess = true, None)
          
          override def onFailure(token: IMqttToken, value: Throwable) = {
            println("CONN: Publish failed " + connection.getClientId)
            value.printStackTrace()
            writeData(isSuccess = false, Some(value.getMessage))
          }

          private def writeData(isSuccess: Boolean, message: Option[String]) = {
            statsEngine.logResponse(
                session, requestName, ResponseTimings(requestStartDate,nowMillis) ,
                if (isSuccess) OK else KO,
                None, message
            )
            next ! session
            mqttProtocol.socketPart.shareConnection match {
              case Some(true) => // nothing
              case _ => connection.disconnect
            }
          }
        }
      )
      } catch {
        case NonFatal(e) => 
          e.printStackTrace()
          throw e
      }
    }
  }
}
