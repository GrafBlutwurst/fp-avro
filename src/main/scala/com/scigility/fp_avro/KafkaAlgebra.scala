package com
package scigility
package fp_avro

import java.nio.charset.Charset

import scalaz.Scalaz._
import scalaz._
import scodec.bits.ByteVector

final case class JsonAvroMessage(schemaId:Int, payload:String)

/**
  * This is the Algebra we require for Kafka Message handling. we assume the following format '{ "schemaID":0, "payload":{...} }'
  **/
trait KafkaAlgebra[F[_]] {
  def readJsonAvroMessage(bytes:ByteVector):F[JsonAvroMessage]
}

object KafkaAlgebra {
  def eKafkaAlgebra = new KafkaAlgebra[String \/ ?] {
    override def readJsonAvroMessage(bytes: ByteVector): String \/ JsonAvroMessage = for {
       schemaID <- bytes.headOption.fold(
          "ByteVector was empty. Could not read schema byte".left[Int]
       )(_.toInt.right[String])

       payload <- bytes.tail.decodeString(Charset.forName("UTF-8")).leftMap(_.toString).fold(_.left[String], _.right[String]) //Charset.forName can throw. but it's static so it's "fine"
    } yield JsonAvroMessage(schemaID, payload)
  }



  def ntAlgebra[F[_], G[_]](alg:KafkaAlgebra[F])(nt: F ~> G):KafkaAlgebra[G] = new KafkaAlgebra[G] {
    override def readJsonAvroMessage(bytes: ByteVector): G[JsonAvroMessage] = nt(alg.readJsonAvroMessage(bytes))
  }

}