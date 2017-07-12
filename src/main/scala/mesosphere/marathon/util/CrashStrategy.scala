package mesosphere.marathon
package util

import akka.Done
import mesosphere.marathon.core.async.ExecutionContexts
import mesosphere.marathon.core.base._

import scala.concurrent.Future

trait CrashStrategy {
  def crash(): Future[Done]
}

object JvmExitsCrashStrategy extends CrashStrategy {
  override def crash(): Future[Done] = {
    Runtime.getRuntime.asyncExit()(ExecutionContexts.global)
  }
}

object TestingCrashStrategy extends CrashStrategy {
  override def crash(): Future[Done] = Future.successful(Done)
}
