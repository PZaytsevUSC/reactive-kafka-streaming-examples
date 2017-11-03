package streaming.stockmarket

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.file.Paths

import akka.actor.{Actor, ActorLogging, ActorSystem, Cancellable, Props}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerMessage, ConsumerSettings, ProducerSettings, Subscriptions}
import akka.stream.scaladsl.{Broadcast, FileIO, Flow, Framing, GraphDSL, RunnableGraph, Sink, Source, ZipWith}
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.util.ByteString
import akka.{Done, NotUsed}
import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringSerializer}
import streaming.wordcount.StopStream

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Streaming stock application that calculates tick statistics for 5 seconds window
  *
  */

case class Stock(ticker: String, price: Double, size: Int)
case class StockStat(minAsk: Double, numTrades: Int, avgAsk: Double)

object StockConsumer{
  def apply()(implicit sys: ActorSystem) = sys.actorOf(Props[StockConsumer])
}

class StockConsumer extends Actor with ActorLogging{
  implicit val sys = context.system
  implicit val disp = context.dispatcher
  implicit val mat = ActorMaterializer()

  val consumerSettings = ConsumerSettings(sys, new ByteArrayDeserializer(), new ByteArrayDeserializer())
      .withBootstrapServers("localhost:9092").withGroupId("stocks").withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  val source = Consumer.committableSource(consumerSettings, Subscriptions.topics("Stocks"))
  val flow1 = Flow[ConsumerMessage.CommittableMessage[Array[Byte], Array[Byte]]].map{ msg => msg.record.value()}
  val flow2 = Flow[Array[Byte]].map{ array =>
    val bais = new ByteArrayInputStream(array)
    val input = AvroInputStream.binary[Stock](bais)
    input.iterator.toSeq.head
  }


  val sink = Sink.foreach[Stock](println)
  val statSink = Sink.foreach[StockStat](println)

  val statsGraph = RunnableGraph.fromGraph(GraphDSL.create(){
    implicit b =>
      import GraphDSL.Implicits._
      val broadcast = b.add(Broadcast[Seq[Stock]](3, false))
      val zip  = b.add(ZipWith[Double, Int, Double, StockStat]((a, b, c) => new StockStat(a, b, c)))

      val groupedFlow: Flow[Stock, Seq[Stock], NotUsed] = Flow[Stock].groupedWithin(1000000, 5 seconds)
      val minFlow: Flow[Seq[Stock], Double, NotUsed] = Flow[Seq[Stock]].map(x => x.map(stock => stock.price).min)
      val numTrades: Flow[Seq[Stock], Int, NotUsed] = Flow[Seq[Stock]].map(x => x.length)
      val avgAskPrice: Flow[Seq[Stock], Double, NotUsed] = Flow[Seq[Stock]].map { x =>

        val length = x.length
        val sumAskPrice = x.map(a => a.price).sum
        sumAskPrice / length
      }

      source ~> flow1 ~> flow2 ~> groupedFlow ~> broadcast.in
      broadcast.out(0) ~> minFlow ~> zip.in0
      broadcast.out(1) ~> numTrades ~> zip.in1
      broadcast.out(2) ~> avgAskPrice ~> zip.in2
      zip.out ~> statSink
      ClosedShape
  })




  val g = RunnableGraph.fromGraph(GraphDSL.create(sink){
    implicit b => s =>
      import GraphDSL.Implicits._
      source ~> flow1 ~> flow2 ~> s.in
      ClosedShape
  })


//  g.run().onComplete {
//    _ => sys.terminate()
//  }


  statsGraph.run()

  def receive = {
    case StopStream => log.info("Terminating"); sys.terminate()
  }
}

object StockProducer{
  def apply()(implicit sys: ActorSystem) = sys.actorOf(Props[StockProducer])
}

class StockProducer extends Actor with ActorLogging{
  implicit val sys = context.system
  implicit val disp = context.dispatcher
  implicit val mat = ActorMaterializer()


  val producerSettings = ProducerSettings(sys, new StringSerializer, new ByteArraySerializer).withBootstrapServers("localhost:9092")
  val topic = "Stocks"

  val file = Paths.get("stocks.txt")

  val fileSource = FileIO.fromPath(file).via(Framing.delimiter(ByteString("\n"), 256, true))
    .map(_.utf8String.split(",")).collect {
    case a: Array[String] => new Stock(a(0), a(1).toDouble, a(2).toInt)}

  val infiniteStockSource: Source[Stock, Cancellable] = Source.tick(0 seconds, 100 millisecond, ()).map(_ => StockGenerator.generateNextStock())

  def runWithSource(source: Source[Stock, _]) = {


    val avroPipe = Flow[Stock].map{x =>
      val baos = new ByteArrayOutputStream()
      val output = AvroOutputStream.binary[Stock](baos)
      output.write(x)
      output.close()
      val result = baos.toByteArray
      baos.close()
      result
    }

    val byteFlow:Flow[Array[Byte], ProducerRecord[String, Array[Byte]], NotUsed] = Flow[Array[Byte]].map {array => new ProducerRecord[String, Array[Byte]](topic, "stocks", array)}

    val sink: Sink[ProducerRecord[String, Array[Byte]], Future[Done]] = Producer.plainSink(producerSettings)

    val g: RunnableGraph[Future[Done]] = RunnableGraph.fromGraph(GraphDSL.create(sink){
      import GraphDSL.Implicits._
      implicit b =>
        // retain materialized value of a sink
        s =>
          source  ~> avroPipe ~> byteFlow ~> s.in
          ClosedShape
    })

    g.run.onComplete{
      case Success(Done) => sys.terminate()
      case Failure(failure) => println("failure")
    }
  }

  def runWithFileSource() = runWithSource(fileSource)
  def runInfinity() = runWithSource(infiniteStockSource)

  runInfinity()


  def receive = {
    case StopStream => log.info("Terminating"); sys.terminate()

  }
}


