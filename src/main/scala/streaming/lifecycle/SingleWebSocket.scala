package streaming.lifecycle

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.{Done, NotUsed}
import akka.http.scaladsl.Http
import akka.stream.{ActorMaterializer, KillSwitches, UniqueKillSwitch}
import akka.stream.scaladsl._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws._
import akka.pattern.{Backoff, BackoffSupervisor}
import streaming.lifecycle.Device._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Random, Success}
/**
  * Stream lifecycle management within an actor
  */


object Data  {
  def apply() = new Data()
}

class Data{

  val random = Random
  def getNext = random.nextInt(100).toString
}

// factory for creating an exponential backoff supervisor for top-level actor device and device
object BackOffSupervisor{
  def create(id: String, endpoint: String)(implicit mat: ActorMaterializer) = {
    BackoffSupervisor.props(Backoff.onFailure(
      Device.start(id, endpoint), childName = id, minBackoff = 1 second,maxBackoff = 30 seconds,randomFactor = 0.2
    ))
  }

}

object Device{
  final case object Upgraded
  final case object Connected
  final case object Terminated
  final case class ConnectionFailure(ex: Throwable)
  final case class FailedUpgrade(statusCode: StatusCode)
  final case class DeviceException(id: String) extends Exception(id)

  def start(id: String, endpoint: String)(implicit mat: ActorMaterializer) = Props(classOf[Device], id, endpoint, mat)
}

class Device(id: String, endpoint: String)(implicit materializer: ActorMaterializer) extends Actor with ActorLogging{
  implicit private val system = context.system
  implicit private val executionContext = system.dispatcher


  // has a websocket
  val webSocket = WebSocketMonitor(id, endpoint, self)

  override def postStop() = {
    log.info(s"$id : Stopping WebSocket connection")
    webSocket.killSwitch.shutdown()
  }

  // throws and exception - gets restarted and restarts the stream
  override def receive: Receive = {
    case Upgraded =>
      log.info(s"$id : WebSocket upgraded")
    case FailedUpgrade(statusCode) =>
      log.error(s"$id : Failed to upgrade WebSocket connection : $statusCode")
      throw DeviceException(id)
    case ConnectionFailure(ex) =>
      log.error(s"$id : Failed to establish WebSocket connection $ex")
      throw DeviceException(id)
      // change state to running when connected
    case Connected =>
      log.info(s"$id : WebSocket connected")
      context.become(running)
  }

  def running: Receive = {
    case Terminated =>
      log.error(s"$id : WebSocket connection terminated")
      throw DeviceException(id)
  }


}
// ws://localhost:9001
object WebSocketMonitor{
  def apply(id: String, endpoint: String, suprervisor: ActorRef)(implicit system: ActorSystem, mat: ActorMaterializer, executionContext: ExecutionContext) = {
    new WebSocketMonitor(id, endpoint, suprervisor)(system, mat, executionContext)
  }
}
class WebSocketMonitor(id: String, endpoint: String, supervisor: ActorRef)(implicit system: ActorSystem, mat: ActorMaterializer, executionContext: ExecutionContext) {

  val websocket = Http().webSocketClientFlow(WebSocketRequest(s"$endpoint/measurement/$id"))
  val data = Data()
  def outgoing = Source.tick(1 seconds, 1 seconds, ()).map(_ => TextMessage(data.getNext))

  val incoming = Flow[Message].collect{
    case TextMessage.Strict(text) => Future.successful(text)
    case TextMessage.Streamed(stream) => stream.runFold("")(_ + _).flatMap(Future.successful)
  }.mapAsync(1)(identity).map(println)

  val ((upgradeResponse, killSwitch), closed): ((Future[WebSocketUpgradeResponse], UniqueKillSwitch), Future[Done]) =
    // keep websocketupgrade
    outgoing.viaMat(websocket)(Keep.right)
    // keep both websocket and killswitch in a tuple
    .viaMat(KillSwitches.single)(Keep.both)
    // keep done
    .via(incoming).toMat(Sink.ignore)(Keep.both).run()

  // from successfull result of upgrade response depending on status create a new future by sending an update to actor
  val connected = upgradeResponse.map{
    upgrade => upgrade.response.status match {
      case StatusCodes.SwitchingProtocols => supervisor ! Upgraded
      case statusCode => supervisor ! FailedUpgrade(statusCode)
    }
  }

  // if success, acknowledge, if not send failure
  connected.onComplete {
    case Success(_) => supervisor ! Connected
    case Failure(ex) => supervisor ! ConnectionFailure(ex)
  }

  // if stream completes, terminate
  closed.map{ _ =>
    supervisor ! Terminated
  }



}
