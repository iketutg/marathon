package mesosphere.marathon
package core.health

import akka.actor.{ ActorRef, ActorSystem, PoisonPill }
import akka.event.EventStream
import akka.stream.ActorMaterializer
import java.util.concurrent.{ CountDownLatch, TimeUnit }

import mesosphere.marathon.core.condition.Condition
import mesosphere.marathon.core.health.impl.HealthCheckActor
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.core.task.Task.LaunchedEphemeral
import mesosphere.marathon.core.task.state.NetworkInfo
import mesosphere.marathon.core.task.termination.KillService
import mesosphere.marathon.core.task.tracker.InstanceTracker
import mesosphere.marathon.state.{ AppDefinition, PathId, Timestamp, UnreachableStrategy }
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import scala.concurrent.Await
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import org.scalatest.mockito.MockitoSugar

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Array(Mode.Throughput, Mode.AverageTime))
@Fork(1)
class HealthCheckActorBenchmark extends MockitoSugar {
  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer = ActorMaterializer()

  @Benchmark
  def checkHealth(hole: Blackhole): Unit = {
    val healthChecks = 20
    val latch = new CountDownLatch(healthChecks)

    val killService = mock[KillService]
    val eventBus = mock[EventStream]

    val appId = PathId("/health-check-app")
    val app = AppDefinition(id = appId)
    val instanceId = Instance.Id.forRunSpec(appId)
    val taskId = Task.Id.forInstanceId(instanceId, None)
    val task = LaunchedEphemeral(
      taskId,
      app.version,
      Task.Status(
        stagedAt = Timestamp.now(),
        condition = Condition.Running,
        networkInfo = NetworkInfo("127.0.0.1", Seq(8080), Seq.empty)
      )
    )

    val instanceMock = Instance(
      instanceId,
      Instance.AgentInfo("127.0.0.1", None, Seq.empty),
      Instance.InstanceState(Condition.Running, Timestamp.now(), None, None),
      Map(taskId -> task),
      app.version,
      UnreachableStrategy.default(false)
    )
    val instanceTracker = new InstanceTracker {
      override def specInstancesSync(pathId: PathId) = ???
      override def specInstances(pathId: PathId)(implicit ec: ExecutionContext): Future[Seq[Instance]] = {
        Future {
          // We want all futures to fire at the same time.
          println(s"Health check: ${latch.getCount} of $healthChecks")
          latch.countDown()
          latch.await()
          Seq(instanceMock)
        }
      }

      override def instance(instanceId: Instance.Id) = Future.successful(Some(instanceMock))

      override def instancesBySpecSync: InstanceTracker.InstancesBySpec = ???
      override def instancesBySpec()(implicit ec: ExecutionContext) = ???

      override def countLaunchedSpecInstancesSync(appId: PathId) = ???
      override def countLaunchedSpecInstances(appId: PathId) = ???

      override def hasSpecInstancesSync(appId: PathId) = ???
      override def hasSpecInstances(appId: PathId)(implicit ec: ExecutionContext) = ???

    }

    val healthCheck = MarathonHttpHealthCheck(
      gracePeriod = 1.seconds,
      interval = 1.seconds,
      timeout = 5.seconds,
      port = Some(8080)
    )

    val ref = system.actorOf(HealthCheckActor.props(app, killService, healthCheck, instanceTracker, eventBus))

    Thread.sleep(10.minutes.toMillis)
    ref.tell(PoisonPill, ActorRef.noSender)
  }

  @TearDown(Level.Trial)
  def shutdown(): Unit = {
    system.terminate()
    Await.ready(system.whenTerminated, 15.seconds)
  }
}
