package mesosphere.marathon
package core.election

import akka.actor.ActorSystem
import akka.event.EventStream
import mesosphere.marathon.core.async.ExecutionContexts
import mesosphere.marathon.core.base.LifecycleState
import mesosphere.marathon.core.base.toRichRuntime
import scala.concurrent.duration._

class ElectionModule(
    config: MarathonConf,
    system: ActorSystem,
    eventStream: EventStream,
    hostPort: String,
    lifecycleState: LifecycleState) {

  lazy val electionBackend = if (config.highlyAvailable()) {
    config.leaderElectionBackend.get match {
      case Some("curator") =>
        ElectionService.curatorElectionStream(config, hostPort)(system)
      case backend: Option[String] =>
        throw new IllegalArgumentException(s"Leader election backend $backend not known!")
    }
  } else {
    ElectionService.psuedoElectionStream()
  }

  private def onSuicide(): Unit = { () =>
    import ExecutionContexts.global
    system.scheduler.scheduleOnce(500.milliseconds) {
      Runtime.getRuntime.asyncExit()
    }
  }

  lazy val service: ElectionService = new ElectionServiceImpl(hostPort, electionBackend, { () => onSuicide() })(system)
}
