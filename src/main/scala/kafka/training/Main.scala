package kafka.training

import java.nio.file.Paths
import java.util.UUID

import akka.{Done, NotUsed}
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.dispatch.sysmsg.Terminate
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffsetBatch}
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.{ActorMaterializer, KillSwitches}
import akka.stream.scaladsl.{FileIO, Flow, Framing, Keep, RunnableGraph, Sink, Source}
import akka.util.ByteString
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import streaming.stockmarket.{StockConsumer, StockProducer}
import streaming.wordcount._

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}
import akka.pattern.ask
import clustering.simple.StartCluster
import clustering.transformation.TransformationObject
import streaming.lifecycle.{Device, StreamController, Webserver}

object Main{

  def main(args: Array[String]): Unit = {
    TransformationObject.start(args)

  }

}
