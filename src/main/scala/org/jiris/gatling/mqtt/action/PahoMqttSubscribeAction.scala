package org.jiris.gatling.mqtt.action

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
import scala.util.parsing.json._
import java.util.UUID
import org.eclipse.paho.client.mqttv3._
import org.fusesource.mqtt.client.CallbackConnection
import org.fusesource.mqtt.client.QoS


/**
 * example: 
 * mqtt("MqttSubCommandsData").subscribe(
      "mqtt/v1/${deviceBeingId}/commands/#",
      """{"payload":""""+"${timestamp}"+""""}""",, // with time stamp payload,
      QoS.AT_MOST_ONCE,
      userName = Some("${deviceBeingId}"),
      password = Some("${deviceAccessToken}"),
      clientId = Some("${deviceClientId}")
     )
 */

class PahoMqttSubscribeAction(
  override val statsEngine: StatsEngine,
  override val mqttAttributes: MqttAttributes,
  override val mqttProtocol: MqttProtocol,
  override val next: Action)
    extends PahoMqttBaseAction(statsEngine, mqttAttributes, mqttProtocol, next) {

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
        subscribeTopic(
          resolvedRequestName,
          connection,
          resolvedTopic,
          mqttAttributes.payload,
          mqttAttributes.qos match {
            case QoS.AT_MOST_ONCE => 0
            case QoS.AT_LEAST_ONCE => 1
            case QoS.EXACTLY_ONCE => 2
          },
          session)
      }
    }
  }

  private def subscribeTopic(
      requestName: String,
      connection: IMqttAsyncClient,
      topic: String,
      expectedPayload: Expression[String],
      qos: Int,
      session: Session): Validation[Unit] = {
    expectedPayload(session).map { resolvedPayload =>
      if(!connection.isConnected()) {
        println("CONN: Disconnected!! " + connection.getClientId)
      }
      try {
      //println("MQTT: Going to subscribe: "+topic)
      connection.unsubscribe(topic)
      connection.subscribe(
        topic,
        qos,
        null,
        new IMqttActionListener {
          override def onSuccess(token: IMqttToken) = returnResponse(isSuccess = true, None)
          
          override def onFailure(token: IMqttToken, value: Throwable) = {
            println("CONN: Subscribe failed " + connection.getClientId)
            value.printStackTrace()
            returnResponse(isSuccess = false, Some(value.getMessage))
          }

          private def returnResponse(isSuccess: Boolean, message: Option[String]) = {
            next ! session
            mqttProtocol.socketPart.shareConnection match {
              case Some(true) => // nothing
              case _ => connection.disconnect
            }
          }
        },
        new IMqttMessageListener {

          // For compare expected payload use.
          // or we should use MqttCallback.messageArrived to parse subscribe message.
          override def messageArrived(topic: String, message: MqttMessage) = {
            val receivedMsg = new String(message.getPayload)
            //println("MQTT: clientId: " + connection.getClientId + " received message: " + receivedMsg + ", Expected Payload: " + resolvedPayload)

            // parse client insert "timestamp" in payload.
            // if not exist, use server requestTime/responrtTime in payload.
            val requestTimestamp = getTimeStamp(new String(message.getPayload))
            
            if (requestTimestamp != 0 ) {
              //println("MQTT: clientId: " + connection.getClientId + " elapsed ms: " + (System.currentTimeMillis - requestTimestamp).toString)
              statsEngine.logResponse(
                session, requestName, ResponseTimings(requestTimestamp, System.currentTimeMillis),
                OK,
                None, Some(receivedMsg)
              )
            } else {
              JSON.parseFull(new String(message.getPayload)) match {  
                case Some(map: Map[String, String]) => 
                  val requestTime = (map("requestTime").toDouble * 1000.0).longValue
                  val responseTime = (map("responseTime").toDouble * 1000.0).longValue
                  val elapsedTime = math.max(1, (responseTime - requestTime).toInt)
                  //println("MQTT: clientId: " + connection.getClientId + " elapsed millisec: " + elapsedTime)
                  
                  statsEngine.logResponse(
                      session, requestName, ResponseTimings(requestTime, responseTime),
                      OK,
                      None, Some(receivedMsg)
                  )
                case _ =>
                  statsEngine.logResponse(
                      session, requestName, ResponseTimings(nowMillis,nowMillis),
                      KO,
                      None, Some(receivedMsg)
                  )
              }
            }
          }

          var getTimeStamp = (payload: String) =>{
            var s:Long = 0
            try {
              JSON.parseFull(payload) match {  
                case Some(map: Map[String, String]) => 
                    s = map("payload").toLong
              }

            } catch {
                case NonFatal(e) => 
                  println(e.printStackTrace())
            }
            s  
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