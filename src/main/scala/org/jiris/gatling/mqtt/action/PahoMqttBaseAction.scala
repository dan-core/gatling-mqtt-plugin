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

case class ConnectionBuilder(client: MqttAsyncClient, options: MqttConnectOptions)

object PahoCallbackConnections {
  private val connsBySession = collection.mutable.Map[String, IMqttAsyncClient]()
  
  private def key(session: Session) = s"${session.scenario}:${session.userId}"
  
  def apply(session: Session) = synchronized {
    connsBySession.get(key(session))
  }

  def +=(tuple: Tuple2[Session, IMqttAsyncClient]) = synchronized {
    connsBySession += key(tuple._1) -> tuple._2
  }
  
  // TODO call this from somewhere to dispose connections after test is done
  def close(session: Session) = {
    connsBySession.get(key(session)) match {
      case Some(connection) => try connection.disconnect catch {
        case NonFatal(e) => // ignore
      }
      case _ => // ignore
    }
  }
}

/**
  *
  */
abstract class PahoMqttBaseAction(
  val statsEngine: StatsEngine,
  val mqttAttributes: MqttAttributes,
  val mqttProtocol: MqttProtocol,
  val next: Action
) extends ExitableAction {


  private def configureHostAndClientId(session: Session, mqttAttributes: MqttAttributes)(mqtt: ConnectionBuilder): Validation[ConnectionBuilder] = {
    mqttProtocol.host match {
      case Some(host) => host(session).flatMap { host =>
        mqttAttributes.clientId match {
          case Some(clientId) => clientId(session).map { clientId => ConnectionBuilder(new MqttAsyncClient(host, clientId), new MqttConnectOptions) } 
          case None => mqttProtocol.optionPart.clientId match {
            case Some(clientId) => clientId(session).map { clientId => ConnectionBuilder(new MqttAsyncClient(host, clientId), new MqttConnectOptions) }
            case None => ConnectionBuilder(new MqttAsyncClient(host, UUID.randomUUID.toString), new MqttConnectOptions)
          }
        }
      }
      case None => ConnectionBuilder(new MqttAsyncClient("tcp://localhost:1883", UUID.randomUUID.toString), new MqttConnectOptions)
    }
  }

  private def configureUserName(session: Session, mqttAttributes: MqttAttributes)(mqtt: ConnectionBuilder): Validation[ConnectionBuilder] = {

    if(!Some(mqttAttributes.username).isEmpty){
      mqttAttributes.username match {
        case Some(username) => username(session).map { resolvedUserName =>
          mqtt.options.setUserName(resolvedUserName)
          mqtt
        } 
        case None => mqtt
      }
    } else {
      mqttProtocol.optionPart.userName match {
        case Some(userName) => userName(session).map { resolvedUserName =>
          mqtt.options.setUserName(resolvedUserName)
          mqtt
        }
        case None => mqtt
      }
    }
  }

  private def configurePassword(session: Session, mqttAttributes: MqttAttributes)(mqtt: ConnectionBuilder): Validation[ConnectionBuilder] = {
     if(!Some(mqttAttributes.password).isEmpty){
      mqttAttributes.password match {
      case Some(password) => password(session).map { resolvedPassword =>
        mqtt.options.setPassword(resolvedPassword.toCharArray)
        mqtt
      } 
      case None => {
        mqtt
      }
      }
     } else {
        mqttProtocol.optionPart.password match {
          case Some(password) => password(session).map { resolvedPassword =>
            mqtt.options.setPassword(resolvedPassword.toCharArray)
            mqtt
          }
          case None => mqtt
        }
     }
  }

  private def configureWillTopic(session: Session)(mqtt: ConnectionBuilder): Validation[ConnectionBuilder] = {
    mqttProtocol.optionPart.willTopic match {
      case Some(willTopic) => willTopic(session).flatMap { resolvedWillTopic =>
        val willQosInt = mqttProtocol.optionPart.willQosInt.getOrElse(0)
        val willRetain = mqttProtocol.optionPart.willRetain.getOrElse(false)
        mqttProtocol.optionPart.willMessage match {
          case Some(willMessage) => willMessage(session).map { resolvedWillMessage =>
            mqtt.options.setWill(resolvedWillTopic, resolvedWillMessage.getBytes, willQosInt, willRetain)
            mqtt
          }
          case None => {
            mqtt.options.setWill(resolvedWillTopic, Array[Byte](), willQosInt, willRetain)
            mqtt
          }
        }
      }
      case None => mqtt
    }
  }

  private def configureVersion(session: Session)(mqtt: ConnectionBuilder): Validation[ConnectionBuilder] = {
    mqttProtocol.optionPart.version match {
      case Some(version) => version(session).map { resolvedVersion =>
        mqtt.options.setMqttVersion(resolvedVersion.toInt)
        mqtt
      }
      case None => mqtt
    }
  }

 private  def configureOptions(mqtt: ConnectionBuilder) = {
    // optionPart
    val cleanSession = mqttProtocol.optionPart.cleanSession
    if (cleanSession.isDefined) {
      mqtt.options.setCleanSession(cleanSession.get)
    }
    val keepAlive = mqttProtocol.optionPart.keepAlive
    if (keepAlive.isDefined) {
      mqtt.options.setKeepAliveInterval(keepAlive.get)
    }
    val receiveBufferSize = mqttProtocol.socketPart.receiveBufferSize
    if (receiveBufferSize.isDefined) {
      val bufferOpts = new DisconnectedBufferOptions
      bufferOpts.setBufferSize(receiveBufferSize.get)
      mqtt.client.setBufferOpts(bufferOpts)
    }
  }
  
  override def name:String =" Name"

  private def createClientAndOptions(session: Session) = {
    configureHostAndClientId(session,mqttAttributes)(null)
      .flatMap(configureUserName(session,mqttAttributes))
      .flatMap(configurePassword(session,mqttAttributes))
      .flatMap(configureWillTopic(session))
      .flatMap(configureVersion(session)).map { builder =>
        configureOptions(builder)
        builder
      }
  }

 override def execute(session: Session):Unit = recover(session) {
    mqttProtocol.socketPart.shareConnection match {
      case Some(true) =>
        PahoCallbackConnections(session) match {
          case Some(connection) => doAction(session, connection)
          case _ =>
            createClientAndOptions(session).map { connection =>
              PahoCallbackConnections += session -> connection.client
              connectAndExecute(session, connection)
            }
        }
      case _ => createClientAndOptions(session).map { connectAndExecute(session, _) }
        
    }
  }

  def connectAndExecute(session: Session, connection: ConnectionBuilder) {
  }

  def doAction(session: Session, connection: IMqttAsyncClient) {
  }
}
