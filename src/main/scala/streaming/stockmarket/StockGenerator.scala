package streaming.stockmarket

import java.nio.file.Paths

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{FileIO, Flow, Keep, Sink, Source}
import akka.stream.{ActorMaterializer, IOResult}
import akka.util.ByteString

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}


object StockGenerator {


  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  val r = scala.util.Random
  val formatter = java.text.NumberFormat.getInstance
  val possible_ticker = (0 to 100).map(x => (r.alphanumeric.head.toString + r.alphanumeric.head.toString + r.alphanumeric.head.toString)toUpperCase())
  val possible_price = (0 to 100).map(x => r.nextDouble)
  val possible_size = (0 to 100).map(x => r.nextInt(100))
  val file = Paths.get("stocks.txt")

  def lineSink(file: String): Sink[String, Future[IOResult]] = {
    Flow[String].map(s => ByteString(s + "\n")).toMat(FileIO.toPath(Paths.get(file)))(Keep.right)

  }

  def generateNextStock() = new Stock((r.alphanumeric.head.toString + r.alphanumeric.head.toString + r.alphanumeric.head.toString)toUpperCase(), r.nextDouble(), r.nextInt(100))

  def mapper(a: (String, Double, Int)): String = a._1 + "," + formatter.format(a._2) + "," + a._3.toString

  def commit() = {

    val s: Source[(String, Double, Int), NotUsed] = Source((possible_ticker, possible_price, possible_size).zipped.toList)
    val f: Flow[(String, Double, Int), String, NotUsed] = Flow[(String, Double, Int)].map(x => mapper(x))
    val sink: Sink[String, Future[IOResult]] = lineSink("stocks.txt")

    s.via(f).runWith(sink).onComplete{
      case Success(x) => println("done"); system.terminate()
      case Failure(failure) => println(failure)
    }

  }

}
