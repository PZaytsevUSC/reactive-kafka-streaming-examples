package streaming.wordcount

import java.util.UUID

import akka.NotUsed
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.Timeout
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}
/**
  * Word count using maps and kafka
  *
  */

object StreamConsumer{
  def start()(implicit sys: ActorSystem) ={
    sys.actorOf(Props[StreamConsumer])
  }
}
class StreamConsumer extends Actor with ActorLogging{
  implicit val sys = context.system
  implicit val disp = context.dispatcher
  implicit val mat = ActorMaterializer()
  val subscription = Subscriptions.topics("test")
  println(UUID.randomUUID().toString())
  val consumerConfigs = ConsumerSettings(sys, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers("localhost:9092")
    .withGroupId("group1")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  def enrichMap(m: Map[String, Int], b: String): Map[String, Int] = {
      val v: Int = m.getOrElse(b, 0) + 1
      m + (b -> v)
  }
  override def preStart(): Unit = {

    val source = Consumer.committableSource(consumerConfigs, subscription)

    val flow: Flow[CommittableMessage[String, String], String, NotUsed] = Flow[CommittableMessage[String, String]].map(msg => msg.record.value())

    val sink: Sink[String, Future[Map[String, Int]]] = Sink.fold(Map.empty[String, Int])((acc: Map[String, Int], str: String) => enrichMap(acc, str))

    val (ls, last): (Control, Future[Map[String, Int]]) = source.via(flow).toMat(sink)(Keep.both).run()
    Thread.sleep(1000)
    ls.shutdown()
    val m = Await.result(last, Timeout(5 seconds).duration)
    println(m)

  }

  def receive = {
    case StopStream => log.info("Terminating"); sys.terminate()
  }
}


case class Increment(value: Int)
case object Get

class MutableActor extends Actor{
  var total: Long = 0
  override def receive: Receive = {
    case Increment(value) =>
      total = total + value
    case Get => println(total)
  }
}

object RunnerActor {
  def start(a: ActorRef)(implicit sys: ActorSystem) = {
    sys.actorOf(Props(new RunnerActor(a)))
  }
}
class RunnerActor(a: ActorRef) extends Actor with ActorLogging{

  implicit val sys = context.system
  implicit val disp = context.dispatcher
  implicit val mat = ActorMaterializer()
  val subscription = Subscriptions.topics("test")
  val consumerConfigs = ConsumerSettings(sys, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("group1")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val done = Consumer.committableSource(consumerConfigs, subscription)
    .via(Flow[CommittableMessage[String, String]]
      .map(msg => msg.record.value()))
    .map{
      case msg: String => a ! Increment(1)
      msg
    }.
    map(println(_)).runWith(Sink.ignore)

  def receive = {
    case StopStream => log.info("Terminating"); sys.terminate()
  }
}