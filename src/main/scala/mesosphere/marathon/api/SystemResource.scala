package mesosphere.marathon
package api

import javax.servlet.http.HttpServletRequest
import javax.ws.rs._
import javax.ws.rs.core.{ Context, MediaType, Response }

import akka.actor.ActorSystem
import ch.qos.logback.classic.{ Level, Logger, LoggerContext }
import com.google.inject.Inject
import com.typesafe.config.{ Config, ConfigRenderOptions }
import com.typesafe.scalalogging.StrictLogging
import com.wix.accord.Validator
import mesosphere.marathon.metrics.Metrics
import mesosphere.marathon.metrics.Spindump;
import mesosphere.marathon.plugin.auth.AuthorizedResource.{ SystemConfig, SystemMetrics }
import mesosphere.marathon.plugin.auth.{ Authenticator, Authorizer, UpdateResource, ViewResource }
import mesosphere.marathon.raml.{ LoggerChange, Raml }
import mesosphere.marathon.raml.MetricsConversion._
import org.slf4j.LoggerFactory
import play.api.libs.json.Json
import stream.Implicits._
import com.wix.accord.dsl._

import scala.concurrent.duration._

/**
  * System Resource gives access to system level functionality.
  * All system resources can be protected via ACLs.
  */
@Path("")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MarathonMediaType.PREFERRED_APPLICATION_JSON))
class SystemResource @Inject() (val config: MarathonConf, cfg: Config)(implicit
  val authenticator: Authenticator,
    val authorizer: Authorizer,
    actorSystem: ActorSystem) extends RestResource with AuthResource with StrictLogging {

  @GET
  @Path("ping")
  @Produces(Array(MediaType.TEXT_PLAIN))
  def ping(): Response = ok("pong")

  @GET
  @Path("metrics")
  def metrics(@Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>
    withAuthorization(ViewResource, SystemMetrics){
      ok(jsonString(Raml.toRaml(Metrics.snapshot())))
    }
  }

  @GET
  @Path("spindump")
  def spindump(@Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>
    withAuthorization(ViewResource, SystemMetrics){
      val queryDuration: Array[String] = req.getParameterValues("duration")
      var duration: Int = 10;

      // Safely process user input regarding the sample duration
      if (queryDuration.length > 0) {
        duration = queryDuration(0).toInt
        if (duration < 0) {
          duration = 0
        }
        if (duration > 30) {
          duration = 30
        }
      }

      // Run spindump and respond
      ok(Spindump.collect(duration))
    }
  }

  @GET
  @Path("config")
  def config(@Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>
    withAuthorization(ViewResource, SystemConfig) {
      ok(cfg.root().render(ConfigRenderOptions.defaults().setJson(true)))
    }
  }

  @GET
  @Path("logging")
  def showLoggers(@Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>
    withAuthorization(ViewResource, SystemConfig) {
      LoggerFactory.getILoggerFactory match {
        case lc: LoggerContext =>
          ok(lc.getLoggerList.map { logger =>
            logger.getName -> Option(logger.getLevel).map(_.levelStr).getOrElse(logger.getEffectiveLevel.levelStr + " (inherited)")
          }.toMap)
      }
    }
  }

  @POST
  @Path("logging")
  def changeLogger(body: Array[Byte], @Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>
    withAuthorization(UpdateResource, SystemConfig) {
      withValid(Json.parse(body).as[LoggerChange]) { change =>
        LoggerFactory.getILoggerFactory.getLogger(change.logger) match {
          case logger: Logger =>
            val level = Level.valueOf(change.level.value.toUpperCase)

            // current level can be null, which means: use the parent level
            // the current level should be preserved, no matter what the effective level is
            val currentLevel = logger.getLevel
            val currentEffectiveLevel = logger.getEffectiveLevel
            logger.info(s"Set logger ${logger.getName} to $level current: $currentEffectiveLevel")
            logger.setLevel(level)

            // if a duration is given, we schedule a timer to reset to the current level
            import mesosphere.marathon.core.async.ExecutionContexts.global
            change.durationSeconds.foreach(duration => actorSystem.scheduler.scheduleOnce(duration.seconds, new Runnable {
              override def run(): Unit = {
                logger.info(s"Duration expired. Reset Logger ${logger.getName} back to $currentEffectiveLevel")
                logger.setLevel(currentLevel)
              }
            }))
            ok(change)
        }
      }
    }
  }

  implicit lazy val validLoggerChange: Validator[LoggerChange] = validator[LoggerChange] { change =>
    change.logger is notEmpty
  }
}
