package com
package scigility
package fp_avro


import cats._
import cats.implicits._
import cats.effect.{Concurrent, ConcurrentEffect, Effect, IO}
import cats.effect.concurrent.{Deferred, Ref}
import com.scigility.algebras.CacheAlgebra
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import eu.timepit.refined._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.string.Url
import org.apache.log4j.Logger
import org.http4s.{Header, Headers, Method, Request, Uri}
import org.http4s.client.blaze._
import org.http4s.circe._
import org.http4s.circe.CirceEntityDecoder._
import io.circe._
import io.circe.generic.auto._

trait SchemaRegistryAlgebra[F[_]] {
  def getSchemaStringForID(id:Int):F[String]
}

object SchemaRegistryAlgebra{


  private final case class SchemaSuccessResponse(schema:String)

  private[this] def appendToUrl[E, M[_]: MonadError[?[_], E]](ef:String => E)(url:String Refined Url, suffix:String):M[String Refined Url] =
    refineV[Url]( if (url.value.endsWith("/")) url.value+suffix else url.value + "/" + suffix).fold[M[String Refined Url]](es => MonadError[M, E].raiseError(ef(es)), MonadError[M, E].pure)

  def schemaRegistryAlgConc[F[_]: ConcurrentEffect](schemaRegistryURL: String Refined Url):F[SchemaRegistryAlgebra[F]] =
    Concurrent[F].product(Http1Client[F](), CacheAlgebra.concMapCached[Int, String, F]).map(
      tpl => new SchemaRegistryAlgebra[F] {
        val client = tpl._1
        val cache = tpl._2


        private[this] def appendToUrlF(url:String Refined Url, suffix:String):F[String Refined Url] = appendToUrl[Throwable, F](es => new RuntimeException(es))(url, suffix)

        override def getSchemaStringForID(id: Int): F[String] = {
          val onMiss = for {
            log <- Slf4jLogger.create[F]
            url <- appendToUrlF(schemaRegistryURL, s"schemas/ids/$id")
            _ <-  log.info("Requesting schema " + url.value)
            resultString <- Uri.fromString(url).fold[F[String]](
              pf => Concurrent[F].raiseError(new RuntimeException(pf)),
              uri => client.expect[SchemaSuccessResponse](
                Request[F](Method.GET, uri).withHeaders(
                  Headers(
                    Header("Accept", "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json")
                  )
                )
              ).map(_.schema.replaceAll("\\\\", ""))
            )
          } yield resultString
          cache.get(onMiss)(id)
        }
      }
    )


}