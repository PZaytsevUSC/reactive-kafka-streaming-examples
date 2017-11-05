package streaming.lifecycle

import akka.{Done, NotUsed}
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.{ActorMaterializer, KillSwitches}
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source}
import akka.util.Timeout
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}
import akka.pattern.ask
/**
  * Inf stream an actor with external control
  */

object StreamController{
  def start()(implicit sys: ActorSystem): ActorRef ={
    sys.actorOf(Props[StreamController])
  }
}

class StreamController extends Actor{

  implicit val sys = context.system
  implicit val disp = context.dispatcher
  implicit val mat = ActorMaterializer()

  private val (killswitch, done) =
    Source.tick(0 seconds, 1 seconds, 1)
      .scan(0)(_ + _)
        .map(_.toString)
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.foreach(println))(Keep.both)
      .run()

  done.map(_ => self ! "done")

  def receive = {
    case "done" => println("Done"); context.stop(self)
    case "stop" =>
      println("Stopping")
      killswitch.shutdown()

  }
}
