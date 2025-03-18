package io.udash.rest

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.eclipse.jetty.ee8.servlet.{ServletContextHandler, ServletHolder}
import org.eclipse.jetty.server.Server

import scala.concurrent.duration.DurationInt

case class User(id: String, login: String)
object User extends RestDataCompanion[User]

trait UserApi2 {
  @GET
  def get(): Task[Observable[User]]
}
object UserApi2 extends DefaultRestApiCompanion[UserApi2]

class UserApiImpl2 extends UserApi2 {
  def get(): Task[Observable[User]] = Task {
    Observable
      .interval(500.millis)
      .map(i => User(s"id-$i", s"login-$i"))
      .take(5)
  }
}

object ServerMain {
  def main(args: Array[String]): Unit = {
    val userApiServlet = RestServlet[UserApi2](new UserApiImpl2)

    val server = new Server(9090)
    val handler = new ServletContextHandler(ServletContextHandler.SESSIONS)
    val servletHolder = new ServletHolder(userApiServlet)
    handler.addServlet(servletHolder, "/*")
    server.setHandler(handler)
    server.start()
    server.join()
  }
}
