package streaming.lifecycle

import akka.{Done, NotUsed}
import akka.actor.{ActorSystem, Props}
import akka.event.Logging
import akka.http.scaladsl.model.ws.BinaryMessage
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Route, RouteResult}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.Timeout

import scala.collection.immutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * Created by pzaytsev on 11/4/17.
  */

object Webserver{
  def start()(implicit system: ActorSystem, mat: ActorMaterializer, executionContext: ExecutionContext) = {
    new Webserver()(system, mat, executionContext)
  }
}
class Webserver()(implicit system: ActorSystem, mat: ActorMaterializer, executionContext: ExecutionContext) {

  val echoSocket: Flow[Message, Message, Any] = {
    Flow[Message].collect {
      case tm: TextMessage => TextMessage(tm.textStream)
      case bm: BinaryMessage => bm.dataStream.runWith(Sink.ignore); TextMessage("nothing")
    }
  }

  val webSocketRoute: Route = path("measurement" / IntNumber){
    id => get{
      handleWebSocketMessages(echoSocket)
    }
  }

  val routeFlow: Flow[HttpRequest, HttpResponse, NotUsed] = RouteResult.route2HandlerFlow(webSocketRoute)


  Http().bindAndHandle(routeFlow, "localhost", 9001)

}
