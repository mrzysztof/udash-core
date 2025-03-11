package io.udash.rest

import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.ee8.servlet.{ServletContextHandler, ServletHolder}

import scala.concurrent.duration.DurationInt

trait UserApi {
  @GET
  def get(): Observable[String]
}
object UserApi extends SDefaultRestApiCompanion[UserApi]

class UserApiImpl extends UserApi {
  def get(): Observable[String] = Observable
    .interval(500.millis)
    .map(i => s"Chunk $i\n")
    .take(25)

}

object ServerMain {
  def main(args: Array[String]): Unit = {
    val userApiServlet = StreamingRestServlet[UserApi](new UserApiImpl)

    val server = new Server(9090)
    val handler = new ServletContextHandler(ServletContextHandler.SESSIONS)
    val servletHolder = new ServletHolder(userApiServlet)
    handler.addServlet(servletHolder, "/*")
    server.setHandler(handler)
    server.start()
    server.join()
  }
}
