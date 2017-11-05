package streaming.wordcount

import java.nio.file.Paths

import akka.{Done, NotUsed}
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.dispatch.sysmsg.Terminate
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.FlowMonitorState.Failed
import akka.stream.{ActorMaterializer, IOResult}
import akka.stream.scaladsl.{FileIO, Framing, Source}
import akka.util.ByteString
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}
import akka.pattern.ask

object StopStream

object StreamProducer {

  def start()(implicit sys: ActorSystem) = {
    sys.actorOf(Props[StreamProducer])
  }
}

class StreamProducer extends Actor with ActorLogging {

  implicit val sys = context.system
  implicit val mat = ActorMaterializer()

  val producerSettings = ProducerSettings(sys,  new StringSerializer, new StringSerializer)
    .withBootstrapServers("localhost:9092")
  val file = Paths.get("stuff.txt")

  override def preStart(): Unit = {
    val pipeline = FileIO.fromPath(file).via(Framing.delimiter(ByteString(" "), 256, true)
      .map(_.utf8String)).map(x=>new ProducerRecord[String, String]("count", x))
    pipeline.runWith(Producer.plainSink(producerSettings))
  }

  def receive = {
    case StopStream => log.info("Terminating"); sys.terminate()
  }
}