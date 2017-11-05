package clustering.transformation
import language.postfixOps
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._
import akka.actor.{Actor, ActorRef, ActorSystem, Props, Terminated}
import akka.util.Timeout
import akka.pattern.ask
import clustering.transformation.TransformationMessages.{BackendRegistration, JobFailed, TransformationJob}
import com.typesafe.config.ConfigFactory

/**
  * Created by pzaytsev on 11/5/17.
  */


object TransformationFrontend{
  def start(a: Seq[String])= {
    val port = if(a.isEmpty) "0" else a(0)
    val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port").
      withFallback(ConfigFactory.parseString("akka.cluster.roles = [frontend]")).
      withFallback(ConfigFactory.load())

    val system = ActorSystem("ClusterSystem", config)
    val frontend = system.actorOf(Props[TransformationFrontend], name = "frontend")
    val counter = new AtomicInteger

    import system.dispatcher

    system.scheduler.schedule(2.seconds, 2.seconds) {
      implicit val timeout = Timeout(5 seconds)
      (frontend ? TransformationJob("hello-" + counter.incrementAndGet())) onSuccess {
        case result => println(result)
      }
    }

  }
}
class TransformationFrontend extends Actor{
  var backends = IndexedSeq.empty[ActorRef]
  var jobCounter = 0

  def receive = {
    case job: TransformationJob if backends.isEmpty =>
      sender() ! JobFailed("Service unavailable, try again later", job)

    case job: TransformationJob =>
      jobCounter += 1
      backends(jobCounter % backends.size) forward job

      // deathwatch: add to watch, remove from services when terminated
      // Death watch uses the cluster failure detector for nodes in the cluster
    case BackendRegistration if !backends.contains(sender()) =>
      context watch sender()
      backends = backends :+ sender()

    case Terminated(a) =>
      backends = backends.filterNot(_ == a)
  }


}
