package com
package scigility
package fp_avro

import cats.effect.IO
import org.http4s.client.blaze.Http1Client

/**
  * All we need here is retrieving a schema by ID. we assume the schema is already in the registry or else emmit an error
  **/
trait SchemaRegistryAlgebra[F[_]] {
  def retrieveSchemaForID(schemaId:Int):F[String]
}

object SchemaRegistryAlgebra {
  //FIXME: dont use String here
  //FIXME: caching
  def ioSchemaRegistryAlrebra(baseURL:String) = new SchemaRegistryAlgebra[IO] {
    override def retrieveSchemaForID(schemaId: Int): IO[String] = for {
      client <- Http1Client[IO]()
      resultString <- client.expect[String](baseURL + "/schemas/ids/" + schemaId.toString)
    } yield resultString
  }

}