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
import org.fusesource.mqtt.client.{MQTT, Callback, QoS, CallbackConnection}
import io.gatling.core.stats.message.ResponseTimings
import org.jiris.gatling.mqtt.protocol.MqttProtocol
import scala.util.control.NonFatal

class MqttRequestAction(
  val statsEngine: StatsEngine,
  val mqtt: MQTT,
  val mqttAttributes: MqttAttributes,
  val mqttProtocol: MqttProtocol,
  val next: Action) extends ExitableAction {

  private def configureHost(session: Session)(mqtt: MQTT): Validation[MQTT] = {
    mqttProtocol.host match {
      case Some(host) => host(session).map { resolvedHost =>
        mqtt.setHost(resolvedHost)
        mqtt
      }
      case None => mqtt
    }
  }

  private def configureClientId(session: Session, mqttAttributes: MqttAttributes)(mqtt: MQTT): Validation[MQTT] = {
    if(!Some(mqttAttributes.clientId).isEmpty){
      mqttAttributes.clientId match {
      case Some(clientId) => clientId(session).map { resolvedClientId =>
        mqtt.setClientId(resolvedClientId)
        mqtt
      } 
      case None => {
        mqtt
      }
      }
     } else {
      mqttProtocol.optionPart.clientId match {
        case Some(clientId) => clientId(session).map { resolvedClientId =>
          mqtt.setClientId(resolvedClientId)
          mqtt
        }
        case None => mqtt
      }
     }
  }

  private def configureUserName(session: Session, mqttAttributes: MqttAttributes)(mqtt: MQTT): Validation[MQTT] = {
    
    if(!Some(mqttAttributes.username).isEmpty){
      mqttAttributes.username match {
      case Some(username) => username(session).map { resolvedUserName =>
        mqtt.setUserName(resolvedUserName)
        mqtt
      } 
      case None => {
        mqtt
      }
      }
    } else {
      mqttProtocol.optionPart.userName match {
      case Some(userName) => userName(session).map { resolvedUserName =>
        mqtt.setUserName(resolvedUserName)
        mqtt
      }
      case None => mqtt
    }
      }
  }

  private def configurePassword(session: Session, mqttAttributes: MqttAttributes)(mqtt: MQTT): Validation[MQTT] = {
     if(!Some(mqttAttributes.password).isEmpty){
      mqttAttributes.password match {
      case Some(password) => password(session).map { resolvedPassword =>
        mqtt.setPassword(resolvedPassword)
        mqtt
      } 
      case None => {
        mqtt
      }
      }
     } else {
        mqttProtocol.optionPart.password match {
          case Some(password) => password(session).map { resolvedPassword =>
            mqtt.setPassword(resolvedPassword)
            mqtt
          }
          case None => mqtt
        }
     }
  }

  private def configureWillTopic(session: Session)(mqtt: MQTT): Validation[MQTT] = {
    mqttProtocol.optionPart.willTopic match {
      case Some(willTopic) => willTopic(session).map { resolvedWillTopic =>
        mqtt.setWillTopic(resolvedWillTopic)
        mqtt
      }
      case None => mqtt
    }
  }

  private def configureWillMessage(session: Session)(mqtt: MQTT): Validation[MQTT] = {
    mqttProtocol.optionPart.willMessage match {
      case Some(willMessage) => willMessage(session).map { resolvedWillMessage =>
        mqtt.setWillMessage(resolvedWillMessage)
        mqtt
      }
      case None => mqtt
    }
  }

  private def configureVersion(session: Session)(mqtt: MQTT): Validation[MQTT] = {
    mqttProtocol.optionPart.version match {
      case Some(version) => version(session).map { resolvedVersion =>
        mqtt.setVersion(resolvedVersion)
        mqtt
      }
      case None => mqtt
    }
  }

  private def configureOptions(mqtt: MQTT) = {
    // optionPart
    val cleanSession = mqttProtocol.optionPart.cleanSession
    if (cleanSession.isDefined) {
      mqtt.setCleanSession(cleanSession.get)
    }
    val keepAlive = mqttProtocol.optionPart.keepAlive
    if (keepAlive.isDefined) {
      mqtt.setKeepAlive(keepAlive.get)
    }
    val willQos = mqttProtocol.optionPart.willQos
    if (willQos.isDefined) {
      mqtt.setWillQos(willQos.get)
    }
    val willRetain = mqttProtocol.optionPart.willRetain
    if (willRetain.isDefined) {
      mqtt.setWillRetain(willRetain.get)
    }

    // reconnectPart
    val connectAttemptsMax = mqttProtocol.reconnectPart.connectAttemptsMax
    if (connectAttemptsMax.isDefined) {
      mqtt.setConnectAttemptsMax(connectAttemptsMax.get)
    }
    val reconnectAttemptsMax = mqttProtocol.reconnectPart.reconnectAttemptsMax
    if (reconnectAttemptsMax.isDefined) {
      mqtt.setReconnectAttemptsMax(reconnectAttemptsMax.get)
    }
    val reconnectDelay = mqttProtocol.reconnectPart.reconnectDelay
    if (reconnectDelay.isDefined) {
      mqtt.setReconnectDelay(reconnectDelay.get)
    }
    val reconnectDelayMax = mqttProtocol.reconnectPart.reconnectDelayMax
    if (reconnectDelayMax.isDefined) {
      mqtt.setReconnectDelayMax(reconnectDelayMax.get)
    }
    val reconnectBackOffMultiplier =
      mqttProtocol.reconnectPart.reconnectBackOffMultiplier
    if (reconnectBackOffMultiplier.isDefined) {
      mqtt.setReconnectBackOffMultiplier(reconnectBackOffMultiplier.get)
    }

    // socketPart
    val receiveBufferSize = mqttProtocol.socketPart.receiveBufferSize
    if (receiveBufferSize.isDefined) {
      mqtt.setReceiveBufferSize(receiveBufferSize.get)
    }
    val sendBufferSize = mqttProtocol.socketPart.sendBufferSize
    if (sendBufferSize.isDefined) {
      mqtt.setSendBufferSize(sendBufferSize.get)
    }
    val trafficClass = mqttProtocol.socketPart.trafficClass
    if (trafficClass.isDefined) {
      mqtt.setTrafficClass(trafficClass.get)
    }

    // throttlingPart
    val maxReadRate = mqttProtocol.throttlingPart.maxReadRate
    if (maxReadRate.isDefined) {
      mqtt.setMaxReadRate(maxReadRate.get)
    }
    val maxWriteRate = mqttProtocol.throttlingPart.maxWriteRate
    if (maxWriteRate.isDefined) {
      mqtt.setMaxWriteRate(maxWriteRate.get)
    }
  }
  
  override def name:String =" Name"
  
  override def execute(session: Session):Unit = recover(session) {
    mqttProtocol.socketPart.shareConnection match {
      case Some(true) =>
        CallbackConnections(session) match {
          case Some(connection) => send(session, connection)
          case _ =>
            createConnection(session).map { connection =>
              CallbackConnections += session -> connection
              connectAndSend(session, connection)
            }
        }
      case _ => createConnection(session).map { connectAndSend(session, _) }
    }
  }

  private def createConnection(session: Session) =
    configureHost(session)(mqtt)
      .flatMap(configureClientId(session,mqttAttributes))
      .flatMap(configureUserName(session,mqttAttributes))
      .flatMap(configurePassword(session,mqttAttributes))
      .flatMap(configureWillTopic(session))
      .flatMap(configureWillMessage(session))
      .flatMap(configureVersion(session)).map { resolvedMqtt =>
        configureOptions(resolvedMqtt)
        resolvedMqtt.callbackConnection()
      }

  private def connectAndSend(session: Session, connection: CallbackConnection) {
    connection.connect(new Callback[Void] {
      override def onSuccess(void: Void): Unit = {
        send(session, connection)
      }
      
      override def onFailure(value: Throwable): Unit = {
        value.printStackTrace()
        mqttAttributes.requestName(session).map { resolvedRequestName =>
        statsEngine.logResponse(session, resolvedRequestName, ResponseTimings(nowMillis,nowMillis) ,
              KO,
          None, Some(value.getMessage))
          }
          next ! session
        connection.disconnect(null)
      }
    })
  }
  
  private def send(session: Session, connection: CallbackConnection) {
    mqttAttributes.requestName(session).flatMap { resolvedRequestName =>
      mqttAttributes.topic(session).flatMap { resolvedTopic =>
        sendRequest(
          resolvedRequestName,
          connection,
          resolvedTopic,
          mqttAttributes.payload,
          mqttAttributes.qos,
          mqttAttributes.retain,
          session)
      }
    }
  }
  
  private def sendRequest(
      requestName: String,
      connection: CallbackConnection,
      topic: String,
      payload: Expression[String],
      qos: QoS,
      retain: Boolean,
      session: Session): Validation[Unit] = {
    payload(session).map { resolvedPayload =>
      val requestStartDate = nowMillis
      connection.publish(
        topic, 
        resolvedPayload.getBytes, 
        qos, 
        retain, 
        new Callback[Void] {
          override def onFailure(value: Throwable): Unit =
            writeData(isSuccess = false, Some(value.getMessage))
          override def onSuccess(void: Void): Unit = {
            writeData(isSuccess = true, None)
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
              case _ => connection.disconnect(null)
            }
          }
        }
      )
    }
  }
}

object CallbackConnections {
  private val connsBySession = collection.mutable.Map[String, CallbackConnection]()
  
  private def key(session: Session) = s"${session.scenario}:${session.userId}"
  
  def apply(session: Session) = synchronized {
    connsBySession.get(key(session))
  }

  def +=(tuple: Tuple2[Session, CallbackConnection]) = synchronized {
    connsBySession += key(tuple._1) -> tuple._2
  }
  
  // TODO call this from somewhere to dispose connections after test is done
  def close(session: Session) = {
    connsBySession.get(key(session)) match {
      case Some(connection) => try connection.disconnect(null) catch {
        case NonFatal(e) => // ignore
      }
      case _ => // ignore
    }
  }
}