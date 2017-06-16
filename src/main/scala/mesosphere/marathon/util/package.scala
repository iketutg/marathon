package mesosphere.marathon

import java.util.concurrent.locks.ReentrantLock

import akka.actor.ActorSystem
import akka.pattern.after
import com.typesafe.config.Config

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.FiniteDuration
import scala.language.implicitConversions

package object util {
  type Success[T] = scala.util.Success[T]
  val Success = scala.util.Success
  type Failure[T] = scala.util.Failure[T]
  val Failure = scala.util.Failure

  implicit def toRichFuture[T](f: Future[T]): RichFuture[T] = new RichFuture(f)
  implicit def toRichLock[T](l: ReentrantLock): RichLock = new RichLock(l)
  implicit def toRichConfig[T](c: Config): RichConfig = new RichConfig(c)

  /**
    * Adds the functionality of a timeout to a future.
    *
    * @param f original future
    * @tparam T type of the future result
    */
  implicit class FutureTimeoutLike[T](f: Future[T]) {
    def withTimeout(timeout: FiniteDuration)(implicit system: ActorSystem, ec: ExecutionContext): Future[T] = {
      Future firstCompletedOf Seq(f, after(timeout, system.scheduler)(Future.failed(TimeoutException("Future timed out"))))
    }
  }
}
