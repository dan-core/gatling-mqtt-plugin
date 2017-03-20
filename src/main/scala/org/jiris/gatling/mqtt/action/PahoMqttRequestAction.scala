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
import org.jiris.gatling.mqtt.protocol.MqttProtocol
import scala.util.control.NonFatal
import java.util.UUID
import org.eclipse.paho.client.mqttv3._
import org.fusesource.mqtt.client.CallbackConnection
import org.fusesource.mqtt.client.QoS

case class ConnectionBuilder(client: MqttAsyncClient, options: MqttConnectOptions)

class PahoMqttRequestAction(
  val statsEngine: StatsEngine,
  val mqttAttributes: MqttAttributes,
  val mqttProtocol: MqttProtocol,
  val next: Action)
    extends ExitableAction  {

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

  private def configureOptions(mqtt: ConnectionBuilder) = {
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
  
  override def execute(session: Session):Unit = recover(session) {
    mqttProtocol.socketPart.shareConnection match {
      case Some(true) =>
        PahoCallbackConnections(session) match {
          case Some(connection) => send(session, connection)
          case _ =>
            createClientAndOptions(session).map { connection =>
              PahoCallbackConnections += session -> connection.client
              connectAndSend(session, connection)
            }
        }
      case _ => createClientAndOptions(session).map { connectAndSend(session, _) }
        
    }
  }

  private def createClientAndOptions(session: Session) =
    configureHostAndClientId(session,mqttAttributes)(null)
      .flatMap(configureUserName(session,mqttAttributes))
      .flatMap(configurePassword(session,mqttAttributes))
      .flatMap(configureWillTopic(session))
      .flatMap(configureVersion(session)).map { builder =>
        configureOptions(builder)
        builder
      }

  private def connectAndSend(session: Session, connection: ConnectionBuilder) {
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
        send(session, token.getClient)
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
  
  private def send(session: Session, connection: IMqttAsyncClient) {
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