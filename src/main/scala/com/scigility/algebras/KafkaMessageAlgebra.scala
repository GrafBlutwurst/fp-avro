package com
package scigility
package fp_avro

import java.nio.charset.Charset

import cats._
import cats.effect.Effect
import cats.implicits._
import scodec.bits.ByteVector

final case class JsonAvroMessage(schemaId:Int, payload:String)

/**
  * This is the Algebra we require for Kafka Message handling. we assume the following format '{ "schemaID":0, "payload":{...} }'
  **/
trait KafkaMessageAlgebra[F[_]] {
  def readJsonAvroMessage(bytes:ByteVector):F[JsonAvroMessage]
}

object KafkaMessageAlgebra {

  def effKafkaAlgebra[F[_]:Effect](delimiter:String) = new KafkaMessageAlgebra[F] {
    override def readJsonAvroMessage(bytes: ByteVector): F[JsonAvroMessage] = for {

       charset <- Effect[F].delay(Charset.forName("UTF-8"))

       payloadString <- bytes
         .decodeString(charset)
         .fold(
           Effect[F].raiseError,
           Effect[F].pure
         )

       splits = payloadString.split(delimiter)

       _ <- if (splits.length != 2) Effect[F].raiseError(new RuntimeException("message was not splittable into 2 by " + delimiter + "msg: " + payloadString)) else Effect[F].pure(())

       schemaID <- Effect[F].delay(splits(0).toInt)


       payload = splits(1)
    } yield JsonAvroMessage(schemaID, payload)
  }


}