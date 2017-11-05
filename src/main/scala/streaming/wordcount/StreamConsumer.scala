package streaming.wordcount

import java.util.UUID

import akka.{Done, NotUsed}
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source}
import akka.util.Timeout
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}
import akka.pattern.ask
/**
  * Word count using maps and kafka
  *
  */

case class UpdateMap(a: String)

object StatePreserver{
  def start()(implicit sys: ActorSystem): ActorRef ={
    sys.actorOf(Props[StatePreserver])
  }
}
class StatePreserver extends Actor {

  private var map = Map.empty[String, Int]

  def update(word: String): Map[String, Int] = {
    var value = map.getOrElse(word, 0) + 1
    map += (word -> value)
    map
  }

  def receive = {
    case UpdateMap(word) => println("received"); sender () ! update(word)
  }
}

object StatefulStreamConsumer{
  def start(stateFulActor: ActorRef)(implicit sys: ActorSystem) = {
    sys.actorOf(Props(new StatefulStreamConsumer(stateFulActor)))
  }
}


class StatefulStreamConsumer(state_preserver: ActorRef) extends Actor with ActorLogging {
  implicit val sys = context.system
  implicit val disp = context.dispatcher
  implicit val mat = ActorMaterializer()
  implicit val timeout = Timeout(2 seconds)
  val subscription = Subscriptions.topics("count")
  val consumerConfigs = ConsumerSettings(sys, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("group1")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  def askable(f: String): Future[Map[String, Int]] = {
    (state_preserver ? UpdateMap(f)).mapTo[Map[String, Int]]
  }

  val source = Consumer.committableSource(consumerConfigs, subscription)
  val flow: Flow[CommittableMessage[String, String], String, NotUsed] = Flow[CommittableMessage[String, String]].map(msg => msg.record.value())
  val stateflow: Flow[String, Map[String, Int], NotUsed] = Flow[String].mapAsync(1)(x => askable(x))

  val sink = Sink.foreach[Map[String, Int]](println)

  val g: RunnableGraph[Future[Done]] = source.via(flow).via(stateflow).toMat(sink)(Keep.right)

  g.run().onComplete{
    _ => println("done")
  }


  def receive = {
    case StopStream => log.info("Terminating"); sys.terminate()
  }
}
