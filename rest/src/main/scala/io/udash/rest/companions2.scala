package io.udash.rest

import com.avsystem.commons.meta.MacroInstances
import io.udash.rest.openapi.OpenApiMetadata
import io.udash.rest.raw.{RestMetadata, StreamingRawRest}

trait SClientInstances[Real] {
  def asReal: StreamingRawRest.AsRealRpc[Real]
  def metadata: RestMetadata[Real]
}
trait SServerInstances[Real] {
  def asRaw: StreamingRawRest.AsRawRpc[Real]
  def metadata: RestMetadata[Real]
}
trait SFullInstances[Real] extends SServerInstances[Real] with SClientInstances[Real]

trait SOpenApiInstances[Real] {
  def openapiMetadata: OpenApiMetadata[Real]
}

trait SOpenApiFullInstances[Real] extends SFullInstances[Real] with SOpenApiInstances[Real]


/** @see [[io.udash.rest.RestApiCompanion RestApiCompanion]] */
abstract class SRestOpenApiCompanion[Implicits, Real](protected val implicits: Implicits)(
  implicit inst: MacroInstances[Implicits, SOpenApiFullInstances[Real]]
) {
  implicit final lazy val restMetadata: RestMetadata[Real] = inst(implicits, this).metadata
  implicit final lazy val restAsRaw: StreamingRawRest.AsRawRpc[Real] = inst(implicits, this).asRaw
  implicit final lazy val restAsReal: StreamingRawRest.AsRealRpc[Real] = inst(implicits, this).asReal
  implicit final lazy val openapiMetadata: OpenApiMetadata[Real] = inst(implicits, this).openapiMetadata

  final def fromHandleRequest(handleRequest: StreamingRawRest.HandleRequest): Real =
    StreamingRawRest.fromHandleRequest(handleRequest)
  final def asHandleRequest(real: Real): StreamingRawRest.HandleRequest =
    StreamingRawRest.asHandleRequest(real)
}




