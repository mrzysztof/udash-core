package io.udash
package rest
package raw

import com.avsystem.commons.*
import com.avsystem.commons.meta.*
import com.avsystem.commons.misc.ImplicitNotFound
import com.avsystem.commons.rpc.*
import io.udash.rest.{BodyTypeTag, RestMethodTag}
import monix.reactive.{Observable, ObservableLike}

import scala.annotation.{implicitNotFound, tailrec}

@methodTag[RestMethodTag]
@methodTag[BodyTypeTag]
trait StreamingRawRest {

  import StreamingRawRest.*

  // declaration order of raw methods matters - it determines their priority!

  @multi @tried
  @tagged[Prefix](whenUntagged = new Prefix)
  @tagged[NoBody](whenUntagged = new NoBody)
  @paramTag[RestParamTag](defaultTag = new Path)
  @unmatched(RawRest.NotValidPrefixMethod)
  @unmatchedParam[Body](RawRest.PrefixMethodBodyParam)
  def prefix(
    @methodName name: String,
    @composite parameters: RestParameters
  ): Try[StreamingRawRest]

  @multi @tried
  @tagged[GET]
  @tagged[NoBody](whenUntagged = new NoBody)
  @paramTag[RestParamTag](defaultTag = new Query)
  @unmatched(RawRest.NotValidGetMethod)
  @unmatchedParam[Body](RawRest.GetMethodBodyParam)
  def get(
    @methodName name: String,
    @composite parameters: RestParameters
  ): Observable[RestResponse]

  @multi @tried
  @tagged[BodyMethodTag](whenUntagged = new POST)
  @tagged[FormBody]
  @paramTag[RestParamTag](defaultTag = new Body)
  @unmatched(RawRest.NotValidFormBodyMethod)
  def handleForm(
    @methodName name: String,
    @composite parameters: RestParameters,
    @multi @tagged[Body] body: Mapping[PlainValue]
  ): Observable[RestResponse]

  @multi @tried
  @tagged[BodyMethodTag](whenUntagged = new POST)
  @tagged[JsonBody](whenUntagged = new JsonBody)
  @paramTag[RestParamTag](defaultTag = new Body)
  @unmatched(RawRest.NotValidHttpMethod)
  def handleJson(
    @methodName name: String,
    @composite parameters: RestParameters,
    @multi @tagged[Body] body: Mapping[JsonValue]
  ): Observable[RestResponse]

  @multi @tried
  @tagged[BodyMethodTag](whenUntagged = new POST)
  @tagged[CustomBody]
  @paramTag[RestParamTag](defaultTag = new Body)
  @unmatched(RawRest.NotValidCustomBodyMethod)
  @unmatchedParam[Body](RawRest.SuperfluousBodyParam)
  def handleCustom(
    @methodName name: String,
    @composite parameters: RestParameters,
    @encoded @tagged[Body] @unmatched(RawRest.MissingBodyParam) body: HttpBody
  ): Observable[RestResponse]

  def asHandleRequest(metadata: RestMetadata[_]): HandleRequest =
    StreamingRawRest.resolveAndHandle(metadata)(handleResolved)

  def handleResolved(request: RestRequest, resolved: ResolvedCall): Observable[RestResponse] = {
    val RestRequest(method, parameters, body) = request
    val ResolvedCall(_, prefixes, finalCall) = resolved
    val HttpCall(finalPathParams, finalMetadata) = finalCall

    def handleBadBody[T](expr: => T): T = try expr catch {
      case NonFatal(cause) => throw new InvalidRpcCall(s"Invalid HTTP body: ${cause.getMessage}", cause)
    }

    @tailrec
    def resolveCall(rawRest: StreamingRawRest, prefixes: List[PrefixCall]): Observable[RestResponse] = prefixes match {
      case PrefixCall(pathParams, pm) :: tail =>
        rawRest.prefix(pm.name, parameters.copy(path = pathParams)) match {
          case Success(nextRawRest) => resolveCall(nextRawRest, tail)
          case Failure(e: HttpErrorException) => Observable.now(e.toResponse)
          case Failure(cause) => Observable.raiseError(cause)
        }
      case Nil =>
        val finalParameters = parameters.copy(path = finalPathParams)
        if (method == HttpMethod.GET) {
          rawRest.get(finalMetadata.name, finalParameters)
        } else if (finalMetadata.customBody)
          rawRest.handleCustom(finalMetadata.name, finalParameters, body)
        else if (finalMetadata.formBody)
          rawRest.handleForm(finalMetadata.name, finalParameters, handleBadBody(HttpBody.parseFormBody(body)))
        else
          rawRest.handleJson(finalMetadata.name, finalParameters, handleBadBody(HttpBody.parseJsonBody(body)))
    }
    try resolved.adjustResponse(resolveCall(this, prefixes)) catch {
      case e: InvalidRpcCall =>
        Observable.now(extractHttpException(e).map(_.toResponse).getOrElse(RestResponse.plain(400, e.getMessage)))
    }
  }

  @tailrec private def extractHttpException(e: Throwable): Opt[HttpErrorException] = e match {
    case null => Opt.Empty
    case e: HttpErrorException => Opt(e)
    case _ => extractHttpException(e.getCause)
  }
}

object StreamingRawRest extends RawRpcCompanion[StreamingRawRest] {
  type HandleRequest = RestRequest => Observable[RestResponse]

  /**
   * Similar to [[io.udash.rest.raw.RawRest.HandleRequest HandleRequest]] but accepts already resolved path as a second argument.
   */
  type HandleResolvedRequest = (RestRequest, ResolvedCall) => Observable[RestResponse]

  type AsObservable[F[_]] = ObservableLike[F]
  trait FromObservable[F[_]] {
    def fromObservable[A](task: Observable[A]): F[A]
  }

  implicit val observableFromObservable: FromObservable[Observable] =
    new FromObservable[Observable] {
      override def fromObservable[A](task: Observable[A]): Observable[A] = task
    }

  final val NotValidPrefixMethod =
    "it cannot be translated into a prefix method"
  final val PrefixMethodBodyParam =
    "prefix methods cannot take @Body parameters"
  final val NotValidGetMethod =
    "it cannot be translated into an HTTP GET method"
  final val GetMethodBodyParam =
    "GET methods cannot take @Body parameters"
  final val NotValidHttpMethod =
    "it cannot be translated into an HTTP method"
  final val NotValidFormBodyMethod =
    "it cannot be translated into an HTTP method with form body"
  final val NotValidCustomBodyMethod =
    "it cannot be translated into an HTTP method with custom body"
  final val MissingBodyParam =
    "expected exactly one @Body parameter but none was found"
  final val SuperfluousBodyParam =
    "expected exactly one @Body parameter but more than one was found"
  final val InvalidTraitMessage =
    "result type ${T} is not a valid REST API trait, does it have a properly defined companion object?"

  @implicitNotFound(InvalidTraitMessage)
  implicit def rawRestAsRealNotFound[T]: ImplicitNotFound[AsReal[StreamingRawRest, T]] = ImplicitNotFound()

  @implicitNotFound(InvalidTraitMessage)
  implicit def rawRestAsRawNotFound[T]: ImplicitNotFound[AsRaw[StreamingRawRest, T]] = ImplicitNotFound()

  def fromHandleRequest[Real: AsRealRpc : RestMetadata](handleRequest: HandleRequest): Real =
    StreamingRawRest.asReal(new DefaultRawRest(Nil, RestMetadata[Real], RestParameters.Empty, handleRequest))

  def asHandleRequest[Real: AsRawRpc : RestMetadata](real: Real): HandleRequest =
    StreamingRawRest.asRaw(real).asHandleRequest(RestMetadata[Real])

  def resolveAndHandle(metadata: RestMetadata[_])(handleResolved: HandleResolvedRequest): HandleRequest = {
    metadata.ensureValid()

    request => {
      val path = request.parameters.path
      metadata.resolvePath(path) match {
        case Nil =>
          val message = s"path ${PlainValue.encodePath(path)} not found"
          Observable.now(RestResponse.plain(404, message))
        case calls => request.method match {
          case HttpMethod.OPTIONS =>
            val meths = calls.iterator.map(_.method).flatMap {
              case HttpMethod.GET => List(HttpMethod.GET, HttpMethod.HEAD)
              case m => List(m)
            } ++ Iterator(HttpMethod.OPTIONS)
            val response = RestResponse(200,
              IMapping.create("Allow" -> PlainValue(meths.mkString(","))), HttpBody.Empty)
            Observable.now(response)
          case wireMethod =>
            val head = wireMethod == HttpMethod.HEAD
            val req = if (head) request.copy(method = HttpMethod.GET) else request
            calls.find(_.method == req.method) match {
              case Some(call) =>
                val resp = handleResolved(req, call)
                if (head) resp.map(_.copy(body = HttpBody.empty)) else resp
              case None =>
                val message = s"$wireMethod not allowed on path ${PlainValue.encodePath(path)}"
                Observable.now(RestResponse.plain(405, message))
            }
        }
      }
    }
  }

  private final class DefaultRawRest(
    prefixMetas: List[PrefixMetadata[_]], //in reverse invocation order!
    metadata: RestMetadata[_],
    prefixParams: RestParameters,
    handleRequest: HandleRequest
  ) extends StreamingRawRest {

    def prefix(name: String, parameters: RestParameters): Try[StreamingRawRest] =
      metadata.prefixesByName.get(name).map { prefixMeta =>
        val newHeaders = prefixParams.append(prefixMeta, parameters)
        Success(new DefaultRawRest(prefixMeta :: prefixMetas, prefixMeta.result.value, newHeaders, handleRequest))
      } getOrElse Failure(new UnknownRpc(name, "prefix"))

    def get(name: String, parameters: RestParameters): Observable[RestResponse] =
      doHandle("get", name, parameters, HttpBody.Empty)

    def handleJson(name: String, parameters: RestParameters, body: Mapping[JsonValue]): Observable[RestResponse] =
      doHandle("handle", name, parameters, HttpBody.createJsonBody(body))

    def handleForm(name: String, parameters: RestParameters, body: Mapping[PlainValue]): Observable[RestResponse] =
      doHandle("handleForm", name, parameters, HttpBody.createFormBody(body))

    def handleCustom(name: String, parameters: RestParameters, body: HttpBody): Observable[RestResponse] =
      doHandle("handleSingle", name, parameters, body)

    private def doHandle(rawName: String, name: String, parameters: RestParameters, body: HttpBody): Observable[RestResponse] =
      metadata.httpMethodsByName.get(name).map { methodMeta =>
        val newHeaders = prefixParams.append(methodMeta, parameters)
        val baseRequest = RestRequest(methodMeta.method, newHeaders, body)
        val request = prefixMetas.foldLeft(methodMeta.adjustRequest(baseRequest))((req, meta) => meta.adjustRequest(req))
        handleRequest(request)
      } getOrElse Observable.raiseError(new UnknownRpc(name, rawName))
  }
}
