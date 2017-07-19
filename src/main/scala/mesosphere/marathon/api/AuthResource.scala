package mesosphere.marathon
package api

import javax.servlet.http.HttpServletRequest
import javax.ws.rs.core.Response

import mesosphere.marathon.AccessDeniedException
import mesosphere.marathon.plugin.auth._
import mesosphere.marathon.plugin.http.HttpResponse

import scala.util.{ Failure, Success, Try }
import scala.collection.JavaConversions._

/**
  * Base trait for authentication and authorization in http resource endpoints.
  */
trait AuthResource extends RestResource {
  implicit val authenticator: Authenticator
  implicit val authorizer: Authorizer

  def authenticated(request: HttpServletRequest)(fn: Identity => Response): Response = {
    val requestWrapper = new RequestFacade(request)
    val authenticationRequest = authenticator.authenticate(requestWrapper)

    Try(result(authenticationRequest)) match {
      case Success(maybeIdentity: Option[Identity]) =>
        maybeIdentity.map { identity =>
          try {
            val result = fn(identity)
            result.getMetadata.put("Marathon-Schema-Version", List(BuildInfo.version))
            result
          } catch {
            case e: AccessDeniedException => withResponseFacade(authorizer.handleNotAuthorized(identity, _))
          }
        }.getOrElse {
          withResponseFacade(authenticator.handleNotAuthenticated(requestWrapper, _))
        }
      case Failure(e) => Response.status(Response.Status.SERVICE_UNAVAILABLE).build()
    }
  }

  def checkAuthorization[T](
    action: AuthorizedAction[T],
    maybeResource: Option[T],
    ifNotExists: Exception)(implicit identity: Identity): Unit = {
    maybeResource match {
      case Some(resource) => checkAuthorization(action, resource)
      case None => throw ifNotExists
    }
  }

  def withAuthorization[A, B >: A](
    action: AuthorizedAction[B],
    maybeResource: Option[A],
    ifNotExists: Response)(fn: A => Response)(implicit identity: Identity): Response =
    {
      maybeResource match {
        case Some(resource) =>
          checkAuthorization(action, resource)
          fn(resource)
        case None => ifNotExists
      }
    }

  def withAuthorization[A, B >: A](
    action: AuthorizedAction[B],
    resource: A)(fn: => Response)(implicit identity: Identity): Response = {
    checkAuthorization(action, resource)
    fn
  }

  def checkAuthorization[A, B >: A](action: AuthorizedAction[B], resource: A)(implicit identity: Identity): A = {
    if (authorizer.isAuthorized(identity, action, resource)) resource
    else throw AccessDeniedException()
  }

  def isAuthorized[T](action: AuthorizedAction[T], resource: T)(implicit identity: Identity): Boolean = {
    authorizer.isAuthorized(identity, action, resource)
  }

  private[this] def withResponseFacade(fn: HttpResponse => Unit): Response = {
    val responseFacade = new ResponseFacade
    fn(responseFacade)
    responseFacade.response
  }
}

